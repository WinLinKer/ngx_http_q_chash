#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>

#define NR_LEAST_VNODE          160
#define NR_VNODE                65536       // 2^16
#define HASH_DATA_LENGTH        32

typedef struct {
    ngx_array_t *values;
    ngx_array_t *lengths;
} ngx_http_upstream_q_chash_srv_conf_t;

typedef struct {
    ngx_int_t peer_index;
    ngx_int_t next;
} q_chash_vnode_t;

typedef struct {
    ngx_http_upstream_rr_peers_t    *peers;
    q_chash_vnode_t                 ring[NR_VNODE];
    ngx_uint_t                      valid_peer_num;
} ngx_http_upstream_q_chash_ring;

typedef struct {
    /* rrp must be first */
    ngx_http_upstream_rr_peer_data_t    rrp;

    ngx_http_upstream_q_chash_ring      *q_chash_ring;
    ngx_uint_t                          hash;
    ngx_uint_t                          tries;
    ngx_event_get_peer_pt               get_rr_peer;
    unsigned                            rr_mode:1;
} ngx_http_upstream_q_chash_peer_data_t;


static void         *ngx_http_upstream_q_chash_create_srv_conf(ngx_conf_t *cf);
static char         *ngx_http_upstream_q_chash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t    ngx_http_upstream_init_q_chash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t    ngx_http_upstream_init_q_chash_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t    ngx_http_upstream_get_q_chash_peer(ngx_peer_connection_t *pc, void *data);
static ngx_http_upstream_rr_peer_t *q_chash_get_peer(ngx_http_upstream_q_chash_peer_data_t *qchp);


static ngx_command_t ngx_http_upstream_q_chash_commands[] = {

    { ngx_string("q_chash"),
        NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
        ngx_http_upstream_q_chash,
        0,
        0,
        NULL },

    ngx_null_command
};

static ngx_http_module_t ngx_http_upstream_q_chash_module_ctx = {
    NULL,                                       /* preconfiguration */
    NULL,                                       /* postconfiguration */

    NULL,                                       /* create main configuration */
    NULL,                                       /* init main configuration */

    ngx_http_upstream_q_chash_create_srv_conf,  /* create server configuration */
    NULL,                                       /* merge server configuration */

    NULL,                                       /* create location configuration */
    NULL                                        /* merge location configuration */
};

ngx_module_t ngx_http_upstream_q_chash_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_q_chash_module_ctx,         /* module context */
    ngx_http_upstream_q_chash_commands,            /* module directives */
    NGX_HTTP_MODULE,                               /* module type */
    NULL,                                          /* init master */
    NULL,                                          /* init module */
    NULL,                                          /* init process */
    NULL,                                          /* init thread */
    NULL,                                          /* exit thread */
    NULL,                                          /* exit process */
    NULL,                                          /* exit master */
    NGX_MODULE_V1_PADDING
};

static void *ngx_http_upstream_q_chash_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_q_chash_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool,
            sizeof(ngx_http_upstream_q_chash_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     *     conf->lengths = NULL;
     *     conf->values = NULL;
     */

    return conf;
}

static ngx_http_upstream_rr_peer_t *q_chash_get_peer(ngx_http_upstream_q_chash_peer_data_t *qchp) {
    ngx_http_upstream_q_chash_ring      *q_chash_ring = qchp->q_chash_ring;
    ngx_http_upstream_rr_peer_data_t    *rrp = &(qchp->rrp);
    ngx_http_upstream_rr_peers_t        *peers = rrp->peers;
    ngx_http_upstream_rr_peer_t         *peer = NULL;
    ngx_uint_t                          i, n, checked;
    uintptr_t                           m;
    time_t                              now;

    now = ngx_time();

    if(q_chash_ring->valid_peer_num == 1) {
        for(i = 0; i < peers->number; i++) {
            peer = &peers->peer[i];
            if(!peer->down) break;
        }
    }
    else if(q_chash_ring->valid_peer_num > 1) {
        for(checked = 0; qchp->tries + checked < q_chash_ring-> valid_peer_num; qchp->hash = q_chash_ring->ring[qchp->hash].next) {

            i = q_chash_ring->ring[qchp->hash].peer_index;

            n = i / (8 * sizeof(uintptr_t));
            m = (uintptr_t) 1 << i % (8 * sizeof(uintptr_t));

            if (rrp->tried[n] & m) {
                continue;
            }

            if(peers->peer[i].max_fails 
                    && peers->peer[i].fails >= peers->peer[i].max_fails
                    && now - peers->peer[i].checked <= peers->peer[i].fail_timeout) {
                rrp->tried[n] |= m;
                checked ++;
                continue;
            }

            qchp->hash = q_chash_ring->ring[qchp->hash].next;
            peer = &peers->peer[i];
            break;
        }
    }

    if(peer == NULL)
        return NULL;

    i = peer - &rrp->peers->peer[0];

    rrp->current = i;

    n = i / (8 * sizeof(uintptr_t));
    m = (uintptr_t) 1 << i % (8 * sizeof(uintptr_t));

    rrp->tried[n] |= m;

    peer->checked = now;

    qchp->tries ++;

    return peer;
}

static ngx_int_t ngx_http_upstream_get_q_chash_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_q_chash_peer_data_t   *qchp = data;
    ngx_http_upstream_q_chash_ring          *q_chash_ring = qchp->q_chash_ring;
    ngx_http_upstream_rr_peer_data_t        *rrp = &(qchp->rrp);
    ngx_http_upstream_rr_peers_t            *peers = rrp->peers;
    ngx_http_upstream_rr_peer_t             *peer;
    ngx_uint_t                              i, n;
    ngx_int_t                               rc;

    ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "q_chash try %ui, valid %ui", qchp->tries, q_chash_ring->valid_peer_num);

    if(!qchp->rr_mode) {
        peer = q_chash_get_peer(qchp);

        if (peer == NULL) {
            if(peers->next) {
                ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "return to rr");
                goto return_to_rr;
            }
            else {
                ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "return to busy");
                goto return_to_busy;
            }
        }

        pc->sockaddr = peer->sockaddr;
        pc->socklen = peer->socklen;
        pc->name = &peer->name;

        ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "q_chash peer, current: %ui, %V", rrp->current, pc->name);

        if (pc->tries == 1 && rrp->peers->next) {
            pc->tries += rrp->peers->next->number;
        }

        return NGX_OK;
    }

return_to_rr:

    qchp->rr_mode = 1;
    if(peers->next) {
        rrp->peers = peers->next;
        pc->tries = rrp->peers->number;
        n = (rrp->peers->number + (8 * sizeof(uintptr_t) - 1))
            / (8 * sizeof(uintptr_t));

        for (i = 0; i < n; i++) {
            rrp->tried[i] = 0;
        }

    }

    rc = qchp->get_rr_peer(pc, rrp);
    if (rc != NGX_BUSY) {
        ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "rr peer, backup current: %ui, %V", rrp->current, pc->name);
        return rc;
    }

return_to_busy:

    /* all peers failed, mark them as live for quick recovery */
    ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "clear fails");
    for (i = 0; i < peers->number; i++) {
        peers->peer[i].fails = 0;
    }

    pc->name = peers->name;

    return NGX_BUSY;
}

static ngx_int_t ngx_http_upstream_init_q_chash_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us)
{
    ngx_int_t                                       rc;
    ngx_http_upstream_q_chash_srv_conf_t            *uchscf;
    ngx_http_upstream_q_chash_peer_data_t           *qchp;
    ngx_http_upstream_q_chash_ring                  *q_chash_ring;
    ngx_str_t                                       evaluated_key_to_hash;

    qchp = ngx_pcalloc(r->pool, sizeof(*qchp));
    if(qchp == NULL)
        return NGX_ERROR;
    r->upstream->peer.data = &qchp->rrp;


    // 对应init_q_chash中的替换
    q_chash_ring = us->peer.data;
    us->peer.data = q_chash_ring->peers;

    qchp->q_chash_ring = q_chash_ring;
    qchp->get_rr_peer = ngx_http_upstream_get_round_robin_peer;
    qchp->tries = 0;
    qchp->rr_mode = 0;

    rc = ngx_http_upstream_init_round_robin_peer(r, us);

    r->upstream->peer.get = ngx_http_upstream_get_q_chash_peer;

    // 替换回
    us->peer.data = q_chash_ring;

    if(rc != NGX_OK)
        return NGX_ERROR;

    // 计算该次访问hash值
    uchscf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_q_chash_module);
    if (uchscf == NULL)
        return NGX_ERROR;
    if (ngx_http_script_run(r, &evaluated_key_to_hash, uchscf->lengths->elts, 0, uchscf->values->elts) == NULL)
        return NGX_ERROR;

    qchp->hash = ngx_crc32_long(evaluated_key_to_hash.data, evaluated_key_to_hash.len) % NR_VNODE;

    ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "q_chash key %V", &evaluated_key_to_hash);
    ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "q_chash key hash %ui", qchp->hash);

    return NGX_OK;
}

static ngx_int_t ngx_http_upstream_init_q_chash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    unsigned char                   hash_data[HASH_DATA_LENGTH] = {};
    ngx_http_upstream_q_chash_ring  *q_chash_ring;
    ngx_http_upstream_rr_peers_t    *peers;
    ngx_uint_t                      vnode_num, i, j;
    ngx_uint_t                      scale;
    ngx_int_t                       min_weight;
    uint64_t                        hash;

    q_chash_ring = ngx_pcalloc(cf->pool, sizeof(*q_chash_ring));
    if(q_chash_ring == NULL)
        return NGX_ERROR;

    if (ngx_http_upstream_init_round_robin(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    us->peer.init = ngx_http_upstream_init_q_chash_peer;

    // 此处替换在init_q_chash_peer前要反向操作一次，确保rr_peer初始化正常
    q_chash_ring->peers = us->peer.data;
    us->peer.data = q_chash_ring;

    // 生成哈希环
    peers = q_chash_ring->peers;

    min_weight = peers->peer[0].weight;

    for(i = 0; i < peers->number; i++) {
        ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V %V weight %i", peers->name, &peers->peer[i].name, peers->peer[i].weight);

        if(min_weight > peers->peer[i].weight)
            min_weight = peers->peer[i].weight;

        if(!peers->peer[i].down)
            q_chash_ring->valid_peer_num ++;
    }

    scale = NR_VNODE * min_weight / peers->total_weight;

    if(scale > NR_LEAST_VNODE) {
        scale = NR_LEAST_VNODE;
    }

    if(scale < NR_LEAST_VNODE) {
        ngx_log_error(NGX_LOG_ERR, cf->log, 0, "upstream %V minimum vnode number '%ui' too small.('%ud * total_weight / min_weight' should be gte %ud)", peers->name, scale, NR_VNODE, NR_LEAST_VNODE);
        return NGX_ERROR;
    }

    ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V min weight %i", peers->name, min_weight);
    ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V valid peer num %ui", peers->name, q_chash_ring->valid_peer_num);

    for(i = 0; i < NR_VNODE; i++) {
        q_chash_ring->ring[i].peer_index = -1;
    }

    for(i = 0; i < peers->number; i++) {
        if(peers->peer[i].down) {
            ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V %V down", peers->name, &peers->peer[i].name);
            continue;
        }
        vnode_num = peers->peer[i].weight * scale / min_weight;
        for(j = 0; j < vnode_num; j++) {
            ngx_snprintf(hash_data, HASH_DATA_LENGTH, "%V-%ui%Z", &peers->peer[i].name, j);
            //hash = ngx_crc32_long(hash_data, ngx_strlen(hash_data)) % NR_VNODE;
            u_char md5[16];
            ngx_md5_t ctx;
            ngx_md5_init(&ctx);
            ngx_md5_update(&ctx, hash_data, ngx_strlen(hash_data));
            ngx_md5_final(md5, &ctx);
            hash = (*(uint64_t *)&md5[0] + *(uint64_t *)&md5[8]) % NR_VNODE;
            q_chash_ring->ring[hash].peer_index = i;
        }
    }

    if(q_chash_ring->valid_peer_num > 1) {
        ngx_int_t fill_index = -1;
        ngx_int_t fill_next = -1;
        for(i = 0; i < NR_VNODE; i++) {
            if(q_chash_ring->ring[i].peer_index == -1) {
                continue;
            }
            else if(fill_index == -1) {
                fill_index = i;
            }
            else if(fill_next == -1) {
                if(q_chash_ring->ring[fill_index].peer_index != q_chash_ring->ring[i].peer_index)
                    fill_next = i;
                else
                    continue;
            }
            else {
                break;
            }
        }

        ngx_int_t si;
        for(si = NR_VNODE - 1; si >= 0; si--) {
            if(q_chash_ring->ring[si].peer_index == -1) {
                q_chash_ring->ring[si].peer_index = q_chash_ring->ring[fill_index].peer_index;
                q_chash_ring->ring[si].next = fill_next;
            }
            else if(q_chash_ring->ring[si].peer_index != q_chash_ring->ring[fill_index].peer_index){
                q_chash_ring->ring[si].next = fill_index;
                fill_next = fill_index;
                fill_index = si;
            }
            else {
                q_chash_ring->ring[si].next = fill_next;
            }
        }
    }


    ngx_uint_t *statistic_array = ngx_pcalloc(cf->pool, sizeof(ngx_uint_t) * peers->number);
    for(i = 0; i < NR_VNODE;i++) {
        statistic_array[q_chash_ring->ring[i].peer_index] ++;
    }
    for(i = 0; i < peers->number; i++) {
        ngx_log_debug(NGX_LOG_INFO, cf->log, 0, "upstream %V %V weight %ui nr_vnode %ui", peers->name, &peers->peer[i].name, peers->peer[i].weight, statistic_array[i]);
    }
    ngx_pfree(cf->pool, statistic_array);

    return NGX_OK;
}

static char *ngx_http_upstream_q_chash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t            *uscf;
    ngx_http_upstream_q_chash_srv_conf_t    *uchscf;
    ngx_str_t                               *value;
    ngx_http_script_compile_t               sc;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
    uchscf = ngx_http_conf_upstream_srv_conf(uscf, ngx_http_upstream_q_chash_module);

    value = cf->args->elts;

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &uchscf->lengths;
    sc.values = &uchscf->values;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    uscf->peer.init_upstream = ngx_http_upstream_init_q_chash;
    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
        | NGX_HTTP_UPSTREAM_WEIGHT
        | NGX_HTTP_UPSTREAM_MAX_FAILS
        | NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
        | NGX_HTTP_UPSTREAM_DOWN
        | NGX_HTTP_UPSTREAM_BACKUP;

    return NGX_CONF_OK;
}
