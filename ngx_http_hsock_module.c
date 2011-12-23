/****************************************************************************
Copyright (c) 2011, Roman Arutyunyan (arut@qip.ru)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the <organization> nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
   
*****************************************************************************/

/*
 True async NGINX upstream module for accessing MySQL via HandlerSocket protocol
*/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#define NGX_HTTP_HSOCK_DEFAULT_CONNECT_TIMEOUT  60000
#define NGX_HTTP_HSOCK_DEFAULT_SEND_TIMEOUT     60000
#define NGX_HTTP_HSOCK_DEFAULT_READ_TIMEOUT     60000

static char* ngx_http_hsock_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_select(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_update(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_insert(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_delete(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_key(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_hsock_subrequest(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_int_t ngx_http_hsock_post_conf(ngx_conf_t *cf);
static void* ngx_http_hsock_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_hsock_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

#define NGX_HTTP_HSOCK_SELECT 1
#define NGX_HTTP_HSOCK_UPDATE 2
#define NGX_HTTP_HSOCK_INSERT 3
#define NGX_HTTP_HSOCK_DELETE 4

#define NGX_HTTP_HSOCK_BUFSIZE 256

struct ngx_http_hsock_value_s {

	ngx_int_t index; /* variable index; NGX_CONF_UNSET in case of literal */

	ngx_str_t value;
};

typedef struct ngx_http_hsock_value_s ngx_http_hsock_value_t;

struct ngx_http_hsock_loc_conf_s {

	ngx_http_upstream_conf_t upstream;

	ngx_str_t db;

	ngx_str_t table;

	ngx_str_t index;

	ngx_array_t where;   /* array of ngx_hsock_value_t */

	ngx_array_t columns; /* array of ngx_str_t */

	ngx_array_t values;  /* array of ngx_hsock_value_t */

	ngx_int_t act;

	ngx_str_t op;        /* = > < >= <= */

	ngx_str_t mop;       /* U + - U? +? -? */

	ngx_int_t limit;

	ngx_int_t offset;

	ngx_str_t uri;       /* subrequest */
};

typedef struct ngx_http_hsock_loc_conf_s ngx_http_hsock_loc_conf_t;

struct ngx_http_hsock_ctx_s {

	ngx_http_request_t *request;

	ngx_int_t open_index;

	ngx_int_t field;

	ngx_int_t errcode;

	ngx_array_t subreq_vars; /* subrequest result: array of ngx_str_t */
};

typedef struct ngx_http_hsock_ctx_s ngx_http_hsock_ctx_t;

/* Module commands */

static ngx_command_t ngx_http_hsock_commands[] = {

	{	ngx_string("hsock_pass"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
		ngx_http_hsock_pass,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_connect_timeout"),
		NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_msec_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, upstream.connect_timeout),
		NULL },

	{	ngx_string("hsock_send_timeout"),
		NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_msec_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, upstream.send_timeout),
		NULL },

	{	ngx_string("hsock_buffer_size"),
		NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_size_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, upstream.buffer_size),
		NULL },

	{	ngx_string("hsock_read_timeout"),
		NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_msec_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, upstream.read_timeout),
		NULL },

	{	ngx_string("hsock_db"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, db),
		NULL },

	{	ngx_string("hsock_table"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, table),
		NULL },

	{	ngx_string("hsock_index"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1, /* default is PRIMARY */
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, index),
		NULL },

	{	ngx_string("hsock_select"),
		NGX_HTTP_LOC_CONF|NGX_CONF_1MORE, /* col1 [col2 [...]] */
		ngx_http_hsock_select,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_update"),
		NGX_HTTP_LOC_CONF|NGX_CONF_2MORE, /* col1 val1 [col2 val2 [...]] */
		ngx_http_hsock_update,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_insert"),
		NGX_HTTP_LOC_CONF|NGX_CONF_2MORE, /* col1 val1 [col2 val2 [...]] */
		ngx_http_hsock_insert,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_delete"),
		NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
		ngx_http_hsock_delete,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_key"),
		NGX_HTTP_LOC_CONF|NGX_CONF_1MORE, /* val1 val2 ... */
		ngx_http_hsock_key,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	{	ngx_string("hsock_op"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, op),
		NULL },

	{	ngx_string("hsock_mop"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, mop),
		NULL },

	{	ngx_string("hsock_limit"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_num_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, limit),
		NULL },

	{	ngx_string("hsock_offset"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
		ngx_conf_set_num_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(ngx_http_hsock_loc_conf_t, offset),
		NULL },

	{	ngx_string("hsock_subrequest"),
		NGX_HTTP_LOC_CONF|NGX_CONF_1MORE,
		ngx_http_hsock_subrequest,
		NGX_HTTP_LOC_CONF_OFFSET,
		0,
		NULL },

	ngx_null_command
};

/* Module context */
static ngx_http_module_t ngx_http_hsock_module_ctx = {

	NULL,                               /* preconfiguration */
	ngx_http_hsock_post_conf,           /* postconfiguration */
	NULL,                               /* create main configuration */
	NULL,                               /* init main configuration */
	NULL,                               /* create server configuration */
	NULL,                               /* merge server configuration */
	ngx_http_hsock_create_loc_conf,     /* create location configuration */
	ngx_http_hsock_merge_loc_conf       /* merge location configuration */
};

/* Module */
ngx_module_t ngx_http_hsock_module = {

	NGX_MODULE_V1,
	&ngx_http_hsock_module_ctx,         /* module context */
	ngx_http_hsock_commands,            /* module directives */
	NGX_HTTP_MODULE,                    /* module type */
	NULL,                               /* init master */
	NULL,                               /* init module */
	NULL,                               /* init process */
	NULL,                               /* init thread */
	NULL,                               /* exit thread */
	NULL,                               /* exit process */
	NULL,                               /* exit master */
	NGX_MODULE_V1_PADDING
};

static ngx_int_t ngx_http_hsock_append_data(ngx_http_request_t *r,
		ngx_chain_t **ll,
		u_char *data,
		ngx_uint_t len)
{
	ngx_chain_t *cl = *ll;
	ngx_buf_t *b = cl->buf;

	if (!len)
		return NGX_OK;

	/* TODO: split data on buffer overflow */
	if (b->end - b->last < (int)len) {
		cl = ngx_alloc_chain_link(r->pool);
		(*ll)->next = cl;
		(*ll) = cl;
		cl->next = NULL;
		cl->buf = ngx_create_temp_buf(r->pool, 
				ngx_max(NGX_HTTP_HSOCK_BUFSIZE, len));
		b = cl->buf;
	}

	ngx_memcpy(b->last, data, len);
	b->last += len;

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_append_value(ngx_http_request_t *r,
		ngx_chain_t **ll,
		ngx_http_hsock_value_t* v)
{
	ngx_http_variable_value_t *vv;

	if (v->index == NGX_CONF_UNSET) {

		/* literal */
		if (ngx_http_hsock_append_data(r, ll, v->value.data, v->value.len) != NGX_OK)
			return NGX_ERROR;

	} else {

		vv = ngx_http_get_indexed_variable(r, v->index);

		if (vv->not_found 
				|| ngx_http_hsock_append_data(r, ll, vv->data, vv->len) != NGX_OK)
			return NGX_ERROR;
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_append_value_list(ngx_http_request_t *r,
		ngx_chain_t **ll,
		ngx_array_t* v)
{
	unsigned n;
	ngx_http_hsock_value_t *vv = v->elts;

	for(n = 0; n < v->nelts; ++n, ++vv) {

		if (n && ngx_http_hsock_append_data(r, ll, (u_char*)"\t", 1) != NGX_OK)
			return NGX_ERROR;

		if (ngx_http_hsock_append_value(r, ll, vv) != NGX_OK)
			return NGX_ERROR;
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_append_string_list(ngx_http_request_t *r,
		ngx_chain_t **ll,
		ngx_array_t* v)
{
	unsigned n;
	ngx_str_t *vv = v->elts;

	for(n = 0; n < v->nelts; ++n, ++vv) {

		if (n && ngx_http_hsock_append_data(r, ll, (u_char*)",", 1) != NGX_OK)
			return NGX_ERROR;

		if (ngx_http_hsock_append_data(r, ll, vv->data, vv->len) != NGX_OK)
			return NGX_ERROR;
	}

	return NGX_OK;
}

/* upstream handlers */
static ngx_int_t ngx_http_hsock_create_request(ngx_http_request_t *r)
{
	ngx_http_hsock_loc_conf_t* hlcf;
	ngx_buf_t *b;
	ngx_chain_t *cl, **ll = &cl;
	ngx_http_hsock_ctx_t *ctx;
	ngx_http_upstream_t  *u;
	ngx_str_t op, mop;
	u_char lim[32];
	ngx_str_t slim;

	u = r->upstream;

	/* We use request_sent flag to make reinit_request get
	   called after peer is chosen from keepalive cache.
	   In this handler we cannot check u->peer.cached
	   and find out if the connection should be
	   initialized with open_index call.
	   Additional reply fields from this request 
	   are read by process_header handler.
	*/

	u->request_sent = 1;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
					"hsock creating request");

	hlcf = ngx_http_get_module_loc_conf(r, ngx_http_hsock_module);

	ctx = ngx_http_get_module_ctx(r, ngx_http_hsock_module);

	/* create first chain link & buffer */
	cl = ngx_alloc_chain_link(r->pool);
	b = ngx_create_temp_buf(r->pool, NGX_HTTP_HSOCK_BUFSIZE);
	cl->buf = b;
	cl->next = NULL;

	u->request_bufs = cl;

	op = hlcf->op;

	if (!op.len) {
		ngx_str_set(&op, "=");
	}

	mop = hlcf->mop;

	if (!mop.len) {
		ngx_str_set(&mop, "U");
	}

	slim.len = 0;

	if (hlcf->limit != NGX_CONF_UNSET
			|| hlcf->offset != NGX_CONF_UNSET)
	{
		slim.data = lim;
		slim.len = ngx_snprintf(lim, sizeof(lim), "\t%d\t%d\t", 
				hlcf->limit == NGX_CONF_UNSET ? 1 : hlcf->limit, 
				hlcf->offset == NGX_CONF_UNSET ? 0 : hlcf->offset) - slim.data;
	}

	/* append request */
	switch(hlcf->act) {

		case NGX_HTTP_HSOCK_SELECT:
			b->last = ngx_slprintf(b->last, b->end, "0\t%V\t%d\t", &op, hlcf->where.nelts);
			if (b->last == b->end)
				return NGX_ERROR;
			ngx_http_hsock_append_value_list(r, ll, &hlcf->where);
			ngx_http_hsock_append_data(r, ll, slim.data, slim.len);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\n", 1);
			break;

		case NGX_HTTP_HSOCK_UPDATE:
			b->last = ngx_slprintf(b->last, b->end, "0\t%V\t%d\t", &op, hlcf->where.nelts);
			if (b->last == b->end)
				return NGX_ERROR;
			ngx_http_hsock_append_value_list(r, ll, &hlcf->where);
			ngx_http_hsock_append_data(r, ll, slim.data, slim.len);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\t1\t0\t", 5);
			ngx_http_hsock_append_data(r, ll, mop.data, mop.len);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\t", 1);
			ngx_http_hsock_append_value_list(r, ll, &hlcf->values);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\n", 1);
			break;

		case NGX_HTTP_HSOCK_INSERT:
			b->last = ngx_slprintf(b->last, b->end, "0\t+\t%d\t", hlcf->values.nelts);
			if (b->last == b->end)
				return NGX_ERROR;
			ngx_http_hsock_append_value_list(r, ll, &hlcf->values);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\n", 1);
			break;

		case NGX_HTTP_HSOCK_DELETE:
			b->last = ngx_slprintf(b->last, b->end, "0\t%V\t%d\t", &op, hlcf->where.nelts);
			if (b->last == b->end)
				return NGX_ERROR;
			ngx_http_hsock_append_value_list(r, ll, &hlcf->where);
			ngx_http_hsock_append_data(r, ll, (u_char*)"\t1\t0\tD\n", 7);
			break;

		default:
			return NGX_ERROR;
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_reinit_request(ngx_http_request_t *r)
{
	ngx_buf_t *b;
	ngx_chain_t *cl, **ll = &cl;
	ngx_http_upstream_t  *u;
	ngx_http_hsock_loc_conf_t* hlcf;
	ngx_http_hsock_ctx_t *ctx;

	hlcf = ngx_http_get_module_loc_conf(r, ngx_http_hsock_module);

	ctx = ngx_http_get_module_ctx(r, ngx_http_hsock_module);

	u = r->upstream;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"hsock reinit request (%d)", u->peer.cached);

	if (u->peer.cached)
		return NGX_OK;

	ctx->open_index = 1;

	/* add open_index request */
	cl = ngx_alloc_chain_link(r->pool);
	b = ngx_create_temp_buf(r->pool, NGX_HTTP_HSOCK_BUFSIZE);
	cl->buf = b;
	cl->next = u->request_bufs;
	u->request_bufs = cl;

	b->last = ngx_slprintf(b->last, b->end, "P\t0\t%*s\t%*s\t%*s\t", 
			hlcf->db.len, hlcf->db.data,
			hlcf->table.len, hlcf->table.data,
			hlcf->index.len ? hlcf->index.len : sizeof("PRIMARY") - 1,
				hlcf->index.len ? hlcf->index.data : (u_char*)"PRIMARY"
			);
	if (b->last == b->end)
		return NGX_ERROR;

	ngx_http_hsock_append_string_list(r, ll, &hlcf->columns);
	ngx_http_hsock_append_data(r, ll, (u_char*)"\n", 1);

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_process_header(ngx_http_request_t *r)
{
	ngx_http_upstream_t *u;
	u_char *p, *errmsg;
	ngx_int_t errcode;
	ngx_http_hsock_ctx_t *ctx;
	u_char *pp;

	u = r->upstream;

	ctx = ngx_http_get_module_ctx(r, ngx_http_hsock_module);

	u->headers_in.status_n = 200;
	u->state->status = 200;

	if (!ctx->open_index)
		return NGX_OK;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"hsock processing header");

	pp = u->buffer.pos;

	for(p = pp; p != u->buffer.last && *p != '\n'; ++p);

	if (p == u->buffer.last)
		return NGX_AGAIN;

	u->buffer.pos = ++p;

	for(p = pp; p != u->buffer.last && *p != '\t'; ++p);

	errcode = ngx_atoi(pp, p - pp);

	if (errcode) {

		for(++p; p != u->buffer.last && *p != '\t'; ++p);

		for(errmsg = ++p; p != u->buffer.last && *p != '\t'; ++p);

		ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"hsock server returned error (%d): %*s", 
				errcode, p - errmsg, errmsg);

		return NGX_ERROR;
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_filter_init(void *data)
{
	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_filter(void *data, ssize_t bytes)
{
	ngx_http_upstream_t  *u;
	ngx_http_hsock_ctx_t *ctx = data;
	ngx_buf_t *b;
	ssize_t n;
	ngx_http_request_t *r = ctx->request;
	ngx_chain_t *cl, **ll;
	u_char* fstart;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"hsock filter");

	u = r->upstream;
	b = &u->buffer;

	for (cl = u->out_bufs, ll = &u->out_bufs; cl; cl = cl->next)
		ll = &cl->next;  

	cl = ngx_chain_get_free_buf(r->pool, &u->free_bufs);
	if (cl == NULL)
		return NGX_ERROR;

	*ll = cl;

	cl->buf->flush = 1;
	cl->buf->memory = 1;

	cl->buf->pos = b->last;
	cl->buf->tag = u->output.tag;

	fstart = b->pos;

	u->keepalive = 1;

	for(n = 0; n < bytes; ++n) {

		if (b->pos[n] == '\n') {
			cl->buf->last_buf = 1;
			bytes = n + 1;
			u->length = 0;
			break;
		}

		if (b->pos[n] == '\t')
			b->pos[n] = '\n';
	}

	b->last += bytes;
	cl->buf->last = b->last;

	return NGX_OK;
}

static void ngx_http_hsock_abort_request(ngx_http_request_t *r)
{
	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"hsock abort request");
	return;
}

static void ngx_http_hsock_finalize_request(ngx_http_request_t *r,
		    ngx_int_t rc)
{
	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"hsock finalize request");
	return;
}

/* main request handler */
static ngx_int_t ngx_http_hsock_handler(ngx_http_request_t *r)
{
	ngx_http_hsock_loc_conf_t *hlcf;
	ngx_http_upstream_t *u;
	ngx_int_t rc;
	ngx_http_hsock_ctx_t *ctx;

	hlcf = ngx_http_get_module_loc_conf(r, ngx_http_hsock_module);

	if (hlcf->act == NGX_CONF_UNSET)
		return NGX_DECLINED;

	/* create upstream */

	rc = ngx_http_discard_request_body(r);

	if (rc != NGX_OK)
		return rc;

	if (ngx_http_set_content_type(r) != NGX_OK)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	if (ngx_http_upstream_create(r) != NGX_OK)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	u = r->upstream;

	u->peer.log = r->connection->log;
	u->peer.log_error = NGX_ERROR_ERR;
	u->output.tag = (ngx_buf_tag_t) &ngx_http_hsock_module;

	u->conf = &hlcf->upstream;

	u->create_request = ngx_http_hsock_create_request;
	u->reinit_request = ngx_http_hsock_reinit_request;
	u->process_header = ngx_http_hsock_process_header;
	u->abort_request = ngx_http_hsock_abort_request;
	u->finalize_request = ngx_http_hsock_finalize_request;

	ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_hsock_ctx_t));
	if (ctx == NULL)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	ctx->request = r;

	ngx_http_set_ctx(r, ctx, ngx_http_hsock_module);

	u->input_filter_init = ngx_http_hsock_filter_init;
	u->input_filter = ngx_http_hsock_filter;
	u->input_filter_ctx = ctx;

	r->main->count++;

	ngx_http_upstream_init(r);

	return NGX_DONE;
}

static ngx_int_t ngx_http_hsock_subrequest_done(ngx_http_request_t *r, void *data, ngx_int_t rc)
{
	u_char *s, *st;
	ngx_http_hsock_ctx_t *ctx = data;
	ngx_str_t *val;
	unsigned n = 0;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, 
			"hsock subrequest done; '%*s'", r->upstream->buffer.last - r->upstream->buffer.pos, r->upstream->buffer.pos);

	s = st = r->upstream->buffer.pos; 

	for(;;) {

		if (s == r->upstream->buffer.last || *s == '\n') {

			if (s != st) {
				val = ngx_array_push(&ctx->subreq_vars);
				val->data = st;
				val->len = s - st;

				ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "http subrequest variable #%d='%*s'", n, val->len, val->data);
				++n;
			}

			if (s == r->upstream->buffer.last)
				break;

			st = ++s;

		} else
			++s;
	}

	return NGX_OK;
}

static ngx_int_t ngx_http_hsock_subrequest_handler(ngx_http_request_t *r)
{
	ngx_http_hsock_loc_conf_t *hlcf;
	ngx_http_request_t *sr;
	ngx_http_post_subrequest_t *ps;
	ngx_http_hsock_ctx_t *ctx;

	hlcf = ngx_http_get_module_loc_conf(r, ngx_http_hsock_module);

	if (!hlcf->uri.len)
		return NGX_DECLINED;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "hsock subrequest handler");

	/* check context */
	ctx = ngx_http_get_module_ctx(r, ngx_http_hsock_module);

	if (ctx != NULL && ctx->subreq_vars.nalloc)
		return NGX_DECLINED;

	if (ctx == NULL) {
		ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_hsock_ctx_t));
		ngx_http_set_ctx(r, ctx, ngx_http_hsock_module);
	}

	ngx_array_init(&ctx->subreq_vars, r->pool, 1, sizeof(ngx_str_t));

	ps = ngx_palloc(r->pool, sizeof(ngx_http_post_subrequest_t));
	if (ps == NULL)
		return NGX_ERROR;

	ps->handler = ngx_http_hsock_subrequest_done;
	ps->data = ctx;

	if (ngx_http_subrequest(r, 
			&hlcf->uri, 
			NULL,
			&sr,
			ps,
			NGX_HTTP_SUBREQUEST_WAITED | NGX_HTTP_SUBREQUEST_IN_MEMORY) != NGX_OK)
		return NGX_ERROR;

	if (sr == NULL)
		return NGX_ERROR;

	/* copy request data from parent request */
	sr->args = r->args;
	sr->headers_in = r->headers_in;

	return NGX_DONE;
}

/* configuration */
static ngx_int_t ngx_http_hsock_post_conf(ngx_conf_t *cf)
{
	ngx_http_core_main_conf_t  *cmcf;
	ngx_http_handler_pt *h;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock post conf");

	/* set up handler */
	cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

	h = ngx_array_push(&cmcf->phases[NGX_HTTP_REWRITE_PHASE].handlers);

	if (h == NULL)
		return NGX_ERROR;
	
	*h = ngx_http_hsock_subrequest_handler;

	return NGX_OK;
}

static void* ngx_http_hsock_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_hsock_loc_conf_t *conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_hsock_loc_conf_t));

	conf->upstream.connect_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.send_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.read_timeout = NGX_CONF_UNSET_MSEC;
	conf->upstream.buffer_size = NGX_CONF_UNSET_SIZE;

	conf->act = NGX_CONF_UNSET;

	conf->limit = NGX_CONF_UNSET;
	conf->offset = NGX_CONF_UNSET;

	return conf;
}

static char* ngx_http_hsock_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_hsock_loc_conf_t *prev = parent;
	ngx_http_hsock_loc_conf_t *conf = child;

	ngx_conf_merge_msec_value(conf->upstream.connect_timeout,
			prev->upstream.connect_timeout, NGX_HTTP_HSOCK_DEFAULT_CONNECT_TIMEOUT);

	ngx_conf_merge_msec_value(conf->upstream.send_timeout,
			prev->upstream.send_timeout, NGX_HTTP_HSOCK_DEFAULT_SEND_TIMEOUT);

	ngx_conf_merge_msec_value(conf->upstream.read_timeout,
			prev->upstream.read_timeout, NGX_HTTP_HSOCK_DEFAULT_READ_TIMEOUT);

	ngx_conf_merge_size_value(conf->upstream.buffer_size,
			prev->upstream.buffer_size,
			(size_t) ngx_pagesize);

	if (conf->upstream.upstream == NULL)
		conf->upstream.upstream = prev->upstream.upstream;

	ngx_conf_merge_str_value(conf->db, prev->db, "");
	ngx_conf_merge_str_value(conf->index, prev->index, "");
	ngx_conf_merge_str_value(conf->table, prev->table, "");
	ngx_conf_merge_value(conf->act, prev->act, NGX_CONF_UNSET);
	ngx_conf_merge_str_value(conf->op, prev->op, "");
	ngx_conf_merge_str_value(conf->mop, prev->mop, "");
	ngx_conf_merge_value(conf->limit, prev->limit, NGX_CONF_UNSET);
	ngx_conf_merge_value(conf->offset, prev->offset, NGX_CONF_UNSET);

#define NGX_HTTP_HSOCK_MERGE_ARRAY(name) \
	if (prev->name.nelts && !conf->name.nelts) \
	conf->name = prev->name

	NGX_HTTP_HSOCK_MERGE_ARRAY(columns);
	NGX_HTTP_HSOCK_MERGE_ARRAY(values);
	NGX_HTTP_HSOCK_MERGE_ARRAY(where);

#undef NGX_HTTP_HSOCK_MERGE_ARRAY

	return NGX_CONF_OK;
}

static char* ngx_http_hsock_set_act(ngx_conf_t *cf, int act, void* conf) 
{
	ngx_http_hsock_loc_conf_t *hlcf = conf;

	/* already set ? */
	if (hlcf->act == act)
		return NGX_CONF_OK;

	/* mixed commands ? */
	if (hlcf->act != NGX_CONF_UNSET) 
		return "resets operation";

	hlcf->act = act;

	return NGX_CONF_OK;
}

static ngx_int_t ngx_http_hsock_subrequest_variable_getter(ngx_http_request_t *r,
		    ngx_http_variable_value_t *v, uintptr_t data)
{
	ngx_http_hsock_ctx_t *ctx;
	unsigned n = (unsigned)data;
	ngx_str_t *vals;

	ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, 
			"hsock subrequest accessing variable #%d", (int)data);

	ctx = ngx_http_get_module_ctx(r, ngx_http_hsock_module);

	if (ctx != NULL && n < ctx->subreq_vars.nelts) {
		vals = ctx->subreq_vars.elts;
		vals += n;
		v->data = vals->data;
		v->len = vals->len;
		v->valid = 1;

	} else
		v->not_found = 1;

	return NGX_OK;
}

static char* ngx_http_hsock_select(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_str_t *value = cf->args->elts;
	ngx_http_hsock_loc_conf_t* hlcf = conf;
	unsigned n;
	char* s;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock select command handler");

	if ((s = ngx_http_hsock_set_act(cf, NGX_HTTP_HSOCK_SELECT, conf)) != NGX_CONF_OK)
		return s;

	/* init array */
	if (!hlcf->columns.nalloc && cf->args->nelts > 1)
		ngx_array_init(&hlcf->columns, cf->pool, cf->args->nelts - 1, sizeof(ngx_str_t));

	for(n = 1; n < cf->args->nelts; ++n) {

		if (value[n].data[0] == '$')
			return "contains variables as column names";

		*((ngx_str_t*)ngx_array_push(&hlcf->columns)) = value[n];
	}
	
	return NGX_CONF_OK;
}

static char* ngx_http_hsock_set_columns(ngx_conf_t *cf, void *conf) 
{

	ngx_str_t *value = cf->args->elts;
	ngx_http_hsock_loc_conf_t* hlcf = conf;
	unsigned n;
	ngx_http_hsock_value_t* cvalue;

	if ((cf->args->nelts & 1) != 1) /* odd ? (remember +1 command name */
		return "has odd number of arguments";

	/* init arrays */
	if (cf->args->nelts > 1) {

		if (!hlcf->columns.nalloc)
			ngx_array_init(&hlcf->columns, cf->pool, 
					(cf->args->nelts - 1) / 2, sizeof(ngx_str_t));

		if (!hlcf->values.nalloc)
			ngx_array_init(&hlcf->values, cf->pool, 
					(cf->args->nelts - 1) / 2, sizeof(ngx_http_hsock_value_t));
	}

	for(n = 1; n < cf->args->nelts; ++n) {

		if (value[n].data[0] == '$')
			return "contains variables as column names";

		*((ngx_str_t*)ngx_array_push(&hlcf->columns)) = value[n];

		++n;

		cvalue = ngx_array_push(&hlcf->values);

		if (value[n].len && value[n].data[0] == '$') {
			value[n].len--;
			value[n].data++;
			cvalue->index  = ngx_http_get_variable_index(cf, &value[n]);
		} else {
			cvalue->index = NGX_CONF_UNSET;
			cvalue->value = value[n];
		}
	}
	
	return NGX_CONF_OK;
}

static char* ngx_http_hsock_update(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	char *s;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock update command handler");

	if ((s = ngx_http_hsock_set_act(cf, NGX_HTTP_HSOCK_UPDATE, conf)) != NGX_CONF_OK)
		return s;

	return ngx_http_hsock_set_columns(cf, conf);
}

static char* ngx_http_hsock_insert(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	char *s;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock insert command handler");

	if ((s = ngx_http_hsock_set_act(cf, NGX_HTTP_HSOCK_INSERT, conf)) != NGX_CONF_OK)
		return s;

	return ngx_http_hsock_set_columns(cf, conf);
}

static char* ngx_http_hsock_delete(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	char *s;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock delete command handler");

	if ((s = ngx_http_hsock_set_act(cf, NGX_HTTP_HSOCK_DELETE, conf)) != NGX_CONF_OK)
		return s;

	return NGX_CONF_OK;
}

static char* ngx_http_hsock_key(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_hsock_loc_conf_t* hlcf = conf;
	ngx_str_t *value = cf->args->elts;
	unsigned n;
	ngx_http_hsock_value_t *cvalue;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock where command handler");

	if (!hlcf->where.nalloc && cf->args->nelts > 1) {
		ngx_array_init(&hlcf->where, cf->pool, cf->args->nelts - 1, 
				sizeof(ngx_http_hsock_value_t));
	}
	
	for(n = 1; n < cf->args->nelts; ++n) {

		cvalue = ngx_array_push(&hlcf->where);

		if (value[n].len && value[n].data[0] == '$') {
			value[n].len--;
			value[n].data++;

			cvalue->index = ngx_http_get_variable_index(cf, &value[n]);

		} else {
			cvalue->index = NGX_CONF_UNSET;
			cvalue->value = value[n];
		}
	}

	return NGX_CONF_OK;
}

static char * ngx_http_hsock_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_hsock_loc_conf_t *hlcf = conf;
	ngx_url_t u;
	ngx_str_t *value;
	ngx_http_core_loc_conf_t  *clcf;

	if (hlcf->upstream.upstream)
		return "is duplicate";

	value = cf->args->elts;

	ngx_memzero(&u, sizeof(ngx_url_t));

	u.url = value[1];
	u.no_resolve = 1;

	hlcf->upstream.upstream = ngx_http_upstream_add(cf, &u, 0);
	if (hlcf->upstream.upstream == NULL)
		return NGX_CONF_ERROR;

	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

	clcf->handler = ngx_http_hsock_handler;

	return NGX_CONF_OK;
}

static char * ngx_http_hsock_subrequest(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_hsock_loc_conf_t *hlcf = conf;
	ngx_str_t *value;
	unsigned n;
	ngx_http_variable_t *v;

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock subrequest handler");

	/* create input uri */
	value = cf->args->elts;

	hlcf->uri = value[1];

	ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "hsock subrequest uri: '%V'", &hlcf->uri);

	for(n = 2; n < cf->args->nelts; ++n) {

		if (value[n].len > 0 && value[n].data[0] == '$') {
			++value[n].data;
			--value[n].len;
		}

		v = ngx_http_add_variable(cf, &value[n], NGX_HTTP_VAR_CHANGEABLE);

		v->get_handler = &ngx_http_hsock_subrequest_variable_getter;
		v->data = n - 2;
	}

	return NGX_CONF_OK;
}

