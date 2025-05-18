#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

int ibv_post_recv_ex(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr) {
    return ibv_post_recv(qp, wr, bad_wr);
}

/**
 * Wrapper for ibv_poll_cq
 */
int ibv_poll_cq_ex(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    return ibv_poll_cq(cq, num_entries, wc);
}

/**
 * Wrapper for ibv_post_send
 */
int ibv_post_send_ex(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr) {
    return ibv_post_send(qp, wr, bad_wr);
}

/**
 * Wrapper for ibv_req_notify_cq
 */
int ibv_req_notify_cq_ex(struct ibv_cq *cq, int solicited_only) {
    return ibv_req_notify_cq(cq , solicited_only);
}

/**
 * Wrapper for ibv_get_cq_event
 */
int ibv_get_cq_event_ex(struct ibv_comp_channel *channel, struct ibv_cq **cq, void **cq_context) {
    return ibv_get_cq_event(channel, cq, cq_context);
}

int ibv_query_qp_ex(struct ibv_qp *qp, struct ibv_qp_attr *attr, int attr_mask, struct ibv_qp_init_attr *init_attr) {
    return ibv_query_qp(qp, attr, attr_mask, init_attr);
}
