#include "hrd.h"
#include "main.h"
#include "mica.h"

#define CAS_LOCK
// #define FAA_LOCK

void* run_client(void* arg) {
  struct thread_params params = *(struct thread_params*)arg;
  int clt_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;
  int num_server_ports = params.num_server_ports;

  /* This is the only port used by this client */
  int ib_port_index = params.base_port_index + clt_gid % num_client_ports;
  int srv_virt_port_index = clt_gid % num_server_ports;

  struct hrd_ctrl_blk* cb = hrd_ctrl_blk_init(0, /* local hid */
                            ib_port_index, -1, /* port index, numa node id */
                            NUM_CLIENTS, 0,   /* #conn_qps, use_uc */
                            NULL, RR_SIZE, -1, 0, 0,
                            -1); /* #dgram qps, buf size, shm key */

  memset((void*)cb->conn_buf, 3, RR_SIZE);

  char mstr_qp_name[HRD_QP_NAME_SIZE];
  sprintf(mstr_qp_name, "master-%d-%d", srv_virt_port_index, clt_gid);

  /* Register all created QPs - only some will get used! */
  char clt_conn_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_conn_qp_name, "client-conn-%d", clt_gid);
  hrd_publish_conn_qp(cb, 0, clt_conn_qp_name);

  printf("main: client published all QPs on port %d\n", ib_port_index);


  struct hrd_qp_attr* mstr_qp = NULL;
  while (mstr_qp == NULL) {
    mstr_qp = hrd_get_published_qp(mstr_qp_name);
    if (mstr_qp == NULL) {
      usleep(200000);
    }
  }

  printf("main: Client %s found master! Connecting..\n", clt_conn_qp_name);
  hrd_connect_qp(cb, 0, mstr_qp);
  hrd_wait_till_ready(mstr_qp_name);

  long long rolling_iter = 0; /* For throughput measurement */
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  int batch_size = 1;
  struct ibv_send_wr wr[batch_size], *bad_send_wr = NULL;
  struct ibv_sge sge[batch_size];
  struct ibv_wc wc[WINDOW_SIZE];

#ifdef CAS_LOCK

  uint64_t compare_add, swap;
  compare_add = 0;
  swap = clt_gid;
  while(1){
    memset(wr, 0, sizeof(wr));
    memset(sge, 0, sizeof(sge));
    if (unlikely(rolling_iter >= K_512)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                      (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      
      printf("main: Client %d: %.2f IOPS.\n", clt_gid, K_512 * batch_size / seconds);
      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }

//lock
    while(1){
      sge[0].addr = (uint64_t)((cb->conn_buf));
      sge[0].length = 64;
      sge[0].lkey = cb->conn_buf_mr->lkey;

      wr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      wr[0].wr.atomic.remote_addr = mstr_qp->buf_addr + (clt_gid%8)*1024;
      wr[0].wr.atomic.rkey = mstr_qp->rkey;
      wr[0].wr.atomic.compare_add = compare_add;
      wr[0].wr.atomic.swap = swap;
      wr[0].num_sge = 1;
      wr[0].next = NULL;
      wr[0].sg_list = &sge[0];
      wr[0].send_flags = IBV_SEND_SIGNALED;

      if(ibv_post_send(cb->conn_qp[0], wr, &bad_send_wr)){
        printf("failed send\n");
        return NULL;
      }
      hrd_poll_cq(cb->conn_cq[0], 1, wc);
      uint64_t *rdma_buffer = (uint64_t*)cb->conn_buf;
      if(*rdma_buffer == compare_add)
        break;
      // printf("retry!!\n");
    }

//unlock
    while(1){
      sge[0].addr = (uint64_t)((cb->conn_buf));
      sge[0].length = 64;
      sge[0].lkey = cb->conn_buf_mr->lkey;

      wr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
      wr[0].wr.atomic.remote_addr = mstr_qp->buf_addr + (clt_gid%8)*1024;
      wr[0].wr.atomic.rkey = mstr_qp->rkey;
      wr[0].wr.atomic.compare_add = swap;
      wr[0].wr.atomic.swap = compare_add;
      wr[0].num_sge = 1;
      wr[0].next = NULL;
      wr[0].sg_list = &sge[0];
      wr[0].send_flags = IBV_SEND_SIGNALED;

      if(ibv_post_send(cb->conn_qp[0], wr, &bad_send_wr)){
        printf("failed send\n");
        return NULL;
      }
      hrd_poll_cq(cb->conn_cq[0], 1, wc);
      uint64_t *rdma_buffer = (uint64_t*)cb->conn_buf;
      if(*rdma_buffer == swap)
        break;
    }

    rolling_iter += 1;

  }

#endif

#ifdef FAA_LOCK

  uint64_t compare_add = 1;

  while(1){
    memset(wr, 0, sizeof(wr));
    memset(sge, 0, sizeof(sge));
    if (unlikely(rolling_iter >= K_512)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                      (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      
      printf("main: Client %d: %.2f IOPS.\n", clt_gid, K_512 * batch_size / seconds);
      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }

//lock and get ticket
    sge[0].addr = (uint64_t)((cb->conn_buf));
    sge[0].length = 64;
    sge[0].lkey = cb->conn_buf_mr->lkey;

    wr[0].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr[0].wr.atomic.remote_addr = mstr_qp->buf_addr + (clt_gid%8)*1024;
    wr[0].wr.atomic.rkey = mstr_qp->rkey;
    wr[0].wr.atomic.compare_add = compare_add;
    wr[0].num_sge = 1;
    wr[0].next = NULL;
    wr[0].sg_list = &sge[0];
    wr[0].send_flags = IBV_SEND_SIGNALED;

    if(ibv_post_send(cb->conn_qp[0], wr, &bad_send_wr)){
      printf("failed send\n");
      return NULL;
    }
    hrd_poll_cq(cb->conn_cq[0], 1, wc);

//check ticket
    uint64_t lock_val = *(uint64_t *)(cb->conn_buf);
    uint32_t ticket = lock_val << 32 >> 32;
    uint32_t current = lock_val >> 32; 

    while(ticket != current){
      sge[0].addr = (uint64_t)((cb->conn_buf));
      sge[0].length = 64;
      sge[0].lkey = cb->conn_buf_mr->lkey;

      wr[1].opcode = IBV_WR_RDMA_READ;
      wr[1].wr.rdma.remote_addr = mstr_qp->buf_addr + (clt_gid%8)*1024;
      wr[1].wr.rdma.rkey = mstr_qp->rkey;
      wr[1].num_sge = 1;
      wr[1].next = NULL;
      wr[1].sg_list = &sge[0];
      wr[1].send_flags = IBV_SEND_SIGNALED;

      if(ibv_post_send(cb->conn_qp[0], &(wr[1]), &bad_send_wr)){
        printf("failed send\n");
        return NULL;
      }
      hrd_poll_cq(cb->conn_cq[0], 1, wc);

      lock_val = *(uint64_t *)(cb->conn_buf);
      current = lock_val >> 32; 
    }

//unlock and increase current_number
    sge[0].addr = (uint64_t)((cb->conn_buf));
    sge[0].length = 64;
    sge[0].lkey = cb->conn_buf_mr->lkey;

    wr[0].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr[0].wr.atomic.remote_addr = mstr_qp->buf_addr + (clt_gid%8)*1024;
    wr[0].wr.atomic.rkey = mstr_qp->rkey;
    wr[0].wr.atomic.compare_add = (compare_add << 32);
    wr[0].num_sge = 1;
    wr[0].next = NULL;
    wr[0].sg_list = &sge[0];
    wr[0].send_flags = IBV_SEND_SIGNALED;

    if(ibv_post_send(cb->conn_qp[0], wr, &bad_send_wr)){
      printf("failed send\n");
      return NULL;
    }
    hrd_poll_cq(cb->conn_cq[0], 1, wc);

    lock_val = *(uint64_t *)(cb->conn_buf);
    current = lock_val >> 32; 


    rolling_iter += 1;

  }
#endif

  printf("main: client sleeping\n");
  sleep(1000000);
  return NULL;
}
