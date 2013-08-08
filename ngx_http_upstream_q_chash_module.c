#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>

#define NR_VNODE                160
#define HASH_DATA_LENGTH        32


typedef struct {
    ngx_uint_t  peer_index;
    ngx_uint_t  next;
    uint32_t    point;
} q_chash_vnode_t;

typedef struct {
    q_chash_vnode_t                     *vnodes;
    ngx_uint_t                          nr_vnodes;
    ngx_uint_t                          nr_valid_peers;
} ngx_http_upstream_q_chash_ring;

typedef struct {
    ngx_array_t                         *values;
    ngx_array_t                         *lengths;
    ngx_http_upstream_q_chash_ring      *q_chash_ring;
} ngx_http_upstream_q_chash_srv_conf_t;


typedef struct {
    /* rrp must be first */
    ngx_http_upstream_rr_peer_data_t    rrp;

    ngx_http_upstream_q_chash_ring      *q_chash_ring;
    uint32_t                            point;
    ngx_uint_t                          vnode_index;
    ngx_uint_t                          tries;
    ngx_uint_t                          ignore;
    ngx_event_get_peer_pt               get_rr_peer;
    unsigned                            rr_mode:1;
} ngx_http_upstream_q_chash_peer_data_t;


static void         *ngx_http_upstream_q_chash_create_srv_conf(ngx_conf_t *cf);
static char         *ngx_http_upstream_q_chash(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t    ngx_http_upstream_init_q_chash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t    ngx_http_upstream_init_q_chash_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t    ngx_http_upstream_get_q_chash_peer(ngx_peer_connection_t *pc, void *data);
static int          compare_vnodes_point(const q_chash_vnode_t *n1, const q_chash_vnode_t *n2);
static uint32_t     q_chash_find(const ngx_http_upstream_q_chash_ring *q_chash_ring, uint32_t point);
static ngx_http_upstream_rr_peer_t *q_chash_get_peer(ngx_http_upstream_q_chash_peer_data_t *qchp, ngx_log_t *log);


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
    NULL,                                           /* preconfiguration */
    NULL,                                           /* postconfiguration */

    NULL,                                           /* create main configuration */
    NULL,                                           /* init main configuration */

    ngx_http_upstream_q_chash_create_srv_conf,      /* create server configuration */
    NULL,                                           /* merge server configuration */

    NULL,                                           /* create location configuration */
    NULL                                            /* merge location configuration */
};

ngx_module_t ngx_http_upstream_q_chash_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_q_chash_module_ctx,          /* module context */
    ngx_http_upstream_q_chash_commands,             /* module directives */
    NGX_HTTP_MODULE,                                /* module type */
    NULL,                                           /* init master */
    NULL,                                           /* init module */
    NULL,                                           /* init process */
    NULL,                                           /* init thread */
    NULL,                                           /* exit thread */
    NULL,                                           /* exit process */
    NULL,                                           /* exit master */
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

static uint32_t q_chash_find(const ngx_http_upstream_q_chash_ring *q_chash_ring, uint32_t point) {
    ngx_uint_t mid = 0;
    ngx_uint_t lo = 0;
    ngx_uint_t hi = q_chash_ring->nr_vnodes - 1;

    while(1) {
        if(point <= q_chash_ring->vnodes[lo].point || point > q_chash_ring->vnodes[hi].point) {
            return lo;
        }

        /* test middle point */
        mid = lo + (hi - lo) / 2;

        /* perfect match */
        if (point <= q_chash_ring->vnodes[mid].point &&
                point > (mid ? q_chash_ring->vnodes[mid-1].point : 0)) {
            return mid;
        }

        /* too low, go up */
        if (q_chash_ring->vnodes[mid].point < point) {
            lo = mid + 1;
        }
        else {
            hi = mid - 1;
        }
    }
}

static ngx_http_upstream_rr_peer_t *q_chash_get_peer(ngx_http_upstream_q_chash_peer_data_t *qchp, ngx_log_t *log) {
    ngx_http_upstream_q_chash_ring      *q_chash_ring = qchp->q_chash_ring;
    ngx_http_upstream_rr_peer_data_t    *rrp = &(qchp->rrp);
    ngx_http_upstream_rr_peers_t        *peers = rrp->peers;
    ngx_http_upstream_rr_peer_t         *peer = NULL;
    ngx_uint_t                          i, n;
    uintptr_t                           m;
    time_t                              now;

    now = ngx_time();

    if(q_chash_ring->nr_valid_peers == 1 && qchp->tries < 1) {
        for(i = 0; i < peers->number; i++) {
            peer = &peers->peer[i];
            if(!peer->down) {
                n = i / (8 * sizeof(uintptr_t));
                m = (uintptr_t) 1 << i % (8 * sizeof(uintptr_t));
                break;
            }
        }
    }
    else if(q_chash_ring->nr_valid_peers > 1) {
        for(; qchp->tries + qchp->ignore < q_chash_ring-> nr_valid_peers; qchp->vnode_index = q_chash_ring->vnodes[qchp->vnode_index].next) {

            ngx_log_debug(NGX_LOG_DEBUG_HTTP, log, 0, "q_chash check vnode_index %ui", qchp->vnode_index);

            i = q_chash_ring->vnodes[qchp->vnode_index].peer_index;

            n = i / (8 * sizeof(uintptr_t));
            m = (uintptr_t) 1 << i % (8 * sizeof(uintptr_t));

            if (rrp->tried[n] & m) {
                continue;
            }

            if(peers->peer[i].max_fails 
                    && peers->peer[i].fails >= peers->peer[i].max_fails
                    && now - peers->peer[i].checked <= peers->peer[i].fail_timeout) {
                rrp->tried[n] |= m;
                qchp->ignore ++;
                continue;
            }

            qchp->vnode_index = q_chash_ring->vnodes[qchp->vnode_index].next;
            peer = &peers->peer[i];
            break;
        }
    }

    if(peer == NULL)
        return NULL;

    rrp->current = i;

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

    ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "q_chash try %ui ignore %ui, valid %ui, pc->tries %ui", qchp->tries, qchp->ignore, q_chash_ring->nr_valid_peers, pc->tries);

    if(!qchp->rr_mode) {
        peer = q_chash_get_peer(qchp, pc->log);

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

    // all peers failed, mark them as live for quick recovery
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

    uchscf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_q_chash_module);
    if (uchscf == NULL) {
        return NGX_ERROR;
    }

    q_chash_ring = uchscf->q_chash_ring;

    qchp = ngx_pcalloc(r->pool, sizeof(*qchp));
    if(qchp == NULL)
        return NGX_ERROR;
    r->upstream->peer.data = &qchp->rrp;

    qchp->q_chash_ring = q_chash_ring;
    qchp->get_rr_peer = ngx_http_upstream_get_round_robin_peer;
    qchp->tries = 0;
    qchp->ignore = 0;
    qchp->rr_mode = 0;

    rc = ngx_http_upstream_init_round_robin_peer(r, us);
    if(rc != NGX_OK)
        return NGX_ERROR;

    r->upstream->peer.get = ngx_http_upstream_get_q_chash_peer;

    // calculate the vnode_index
    if(q_chash_ring->nr_valid_peers > 1) {
        if (ngx_http_script_run(r, &evaluated_key_to_hash, uchscf->lengths->elts, 0, uchscf->values->elts) == NULL)
            return NGX_ERROR;

        qchp->point = (uint32_t)ngx_crc32_long(evaluated_key_to_hash.data, evaluated_key_to_hash.len);
        qchp->vnode_index = q_chash_find(q_chash_ring, qchp->point);

        ngx_log_debug(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "q_chash key %V, point %uD, vnode_index %ui", &evaluated_key_to_hash, qchp->point, qchp->vnode_index);
    }

    return NGX_OK;
}

static int compare_vnodes_point(const q_chash_vnode_t *n1, const q_chash_vnode_t *n2) {
    if(n1->point < n2->point)
        return -1;
    else if(n1->point > n2->point)
        return 1;
    return 0;
}

static ngx_int_t ngx_http_upstream_init_q_chash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    unsigned char                         hash_data[HASH_DATA_LENGTH] = {};
    ngx_http_upstream_q_chash_ring        *q_chash_ring;
    ngx_http_upstream_q_chash_srv_conf_t  *uchscf;
    ngx_http_upstream_rr_peers_t          *peers;
    ngx_uint_t                            vnode_num, i, j, k, fill_next;
    ngx_int_t                             si;
    uint32_t                              point;

    uchscf = ngx_http_conf_upstream_srv_conf(us, ngx_http_upstream_q_chash_module);
    if (uchscf == NULL) {
        return NGX_ERROR;
    }

    q_chash_ring = ngx_pcalloc(cf->pool, sizeof(*q_chash_ring));
    if(q_chash_ring == NULL)
        return NGX_ERROR;

    if (ngx_http_upstream_init_round_robin(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    us->peer.init = ngx_http_upstream_init_q_chash_peer;

    uchscf->q_chash_ring = q_chash_ring;

    peers = us->peer.data;

    for(i = 0; i < peers->number; i++) {
        if(!peers->peer[i].down)
            q_chash_ring->nr_valid_peers ++;
    }

    // old_cycle's log_level is NGX_LOG_NOTICE
    //ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V nr_valid_peers %ui", peers->name, q_chash_ring->nr_valid_peers);

    // no need to hash
    if(q_chash_ring->nr_valid_peers <= 1) {
        return NGX_OK;
    }

    // create vnodes, peer_index field, sort
    q_chash_ring->vnodes = ngx_palloc(cf->pool, sizeof(q_chash_vnode_t) * peers->total_weight * NR_VNODE);
    if(q_chash_ring->vnodes == NULL) {
        return NGX_ERROR;
    }

    for(i = 0; i < peers->number; i++) {
        if(peers->peer[i].down) {
            continue;
        }
        vnode_num = peers->peer[i].weight * NR_VNODE;
        for(j = 0; j < vnode_num / 4; j++) {
            ngx_snprintf(hash_data, HASH_DATA_LENGTH, "%V-%ui%Z", &peers->peer[i].name, j);
            u_char md5[16];
            ngx_md5_t ctx;
            ngx_md5_init(&ctx);
            ngx_md5_update(&ctx, hash_data, ngx_strlen(hash_data));
            ngx_md5_final(md5, &ctx);
            for(k = 0; k < 4; k++) {
                point = *(uint32_t *)&md5[k * 4];
                q_chash_ring->vnodes[q_chash_ring->nr_vnodes].peer_index = i;
                q_chash_ring->vnodes[q_chash_ring->nr_vnodes].point = point;
                q_chash_ring->nr_vnodes ++;
            }
        }
    }

    // old_cycle's log_level is NGX_LOG_NOTICE
    //ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "upstream %V nr_vnodes %ui", peers->name, q_chash_ring->nr_vnodes);

    ngx_qsort(q_chash_ring->vnodes, q_chash_ring->nr_vnodes, sizeof(q_chash_vnode_t), (const void *)compare_vnodes_point);

    // fill vnode's next field
    for(i = 1; ; i ++) {
        if(q_chash_ring->vnodes[0].peer_index == q_chash_ring->vnodes[i].peer_index)
            continue;
        q_chash_ring->vnodes[0].next = i;
        break;
    }

    fill_next = 0;

    for(si = q_chash_ring->nr_vnodes - 1; si >= 0; si--) {
        if(q_chash_ring->vnodes[si].peer_index == q_chash_ring->vnodes[fill_next].peer_index) {
            q_chash_ring->vnodes[si].next = q_chash_ring->vnodes[fill_next].next;
        }
        else {
            q_chash_ring->vnodes[si].next = fill_next;
            fill_next = si;
        }
    }

    // old_cycle's log_level is NGX_LOG_NOTICE
    /*
    for(i = 0; i < q_chash_ring->nr_vnodes; i++) {
        ngx_log_debug(NGX_LOG_DEBUG_HTTP, cf->log, 0, "%ui, next %ui peer_index %ui point %uD", i, q_chash_ring->vnodes[i].next, q_chash_ring->vnodes[i].peer_index, q_chash_ring->vnodes[i].point);
    }
    */

    // calculate peer ratio for debug ~
    ngx_uint_t *statistic_array = ngx_pcalloc(cf->pool, sizeof(uint32_t) * peers->number);
    if(statistic_array == NULL)
        return NGX_OK;
    uint32_t before_point = 0;
    for(i = 1; i < q_chash_ring->nr_vnodes; i++) {
        statistic_array[q_chash_ring->vnodes[i].peer_index] += q_chash_ring->vnodes[i].point - before_point;
        before_point = q_chash_ring->vnodes[i].point;
    }
    statistic_array[q_chash_ring->vnodes[0].peer_index] += 0xFFFFFFFF - before_point;
    for(i = 0; i < peers->number; i++) {
        if(peers->peer[i].down)
            continue;
        ngx_log_error(NGX_LOG_NOTICE, cf->log, 0, "upstream %V %V weight %ui actually ratio %.2f%%", peers->name, &peers->peer[i].name, peers->peer[i].weight, 100 * (double)statistic_array[i] / 0xFFFFFFFF);
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
