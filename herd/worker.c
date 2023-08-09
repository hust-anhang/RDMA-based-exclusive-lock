#include "hrd.h"
#include "main.h"
#include "mica.h"

void* run_worker(void* arg) {
  int i;
  struct thread_params params = *(struct thread_params*)arg;
  int wrkr_lid = params.id; /* Local ID of this worker thread*/
  int num_server_ports = params.num_server_ports;
  int base_port_index = params.base_port_index;
  int postlist = params.postlist;

  /*
   * MICA-related checks. Note that @postlist is the largest batch size we
   * feed into MICA. The average postlist per port in a dual-port NIC should
   * be @postlist / 2.
   */
  assert(MICA_MAX_BATCH_SIZE >= postlist);
  assert(HERD_VALUE_SIZE <= MICA_MAX_VALUE);

  assert(UNSIG_BATCH >= postlist); /* Postlist check */
  assert(postlist <= NUM_CLIENTS); /* Static sizing of arrays below */

  /* MICA instance id = wrkr_lid, NUMA node = 0 */
  struct mica_kv kv;
  mica_init(&kv, wrkr_lid, 0, HERD_NUM_BKTS, HERD_LOG_CAP);
  mica_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);

  assert(num_server_ports < MAX_SERVER_PORTS); /* Avoid dynamic alloc */
  struct hrd_ctrl_blk* cb[MAX_SERVER_PORTS];

  for (i = 0; i < num_server_ports; i++) {
    int ib_port_index = base_port_index + i;

    cb[i] = hrd_ctrl_blk_init(wrkr_lid,          /* local_hid */
                              ib_port_index, -1, /* port index, numa node */
                              0, 0,              /* #conn qps, uc */
                              NULL, 0, -1, /*prealloc conn buf, buf size, key */
                              NUM_UD_QPS, 4096,
                              -1); /* num_dgram_qps, dgram_buf_size, key */
  }

  /* Map the request region created by the master */
  volatile struct mica_op* req_buf;
  int sid = shmget(MASTER_SHM_KEY, RR_SIZE, SHM_HUGETLB | 0666);
  assert(sid != -1);
  req_buf = shmat(sid, 0, 0);
  assert(req_buf != (void*)-1);

  /* Create an address handle for each client */
  struct ibv_ah* ah[NUM_CLIENTS];
  memset(ah, 0, NUM_CLIENTS * sizeof(uintptr_t));
  struct hrd_qp_attr* clt_qp[NUM_CLIENTS];

  for (i = 0; i < NUM_CLIENTS; i++) {
    /* Compute the control block and physical port index for client @i */
    int cb_i = i % num_server_ports;
    int local_port_i = base_port_index + cb_i;

    char clt_name[HRD_QP_NAME_SIZE];
    sprintf(clt_name, "client-dgram-%d", i);

    /* Get the UD queue pair for the ith client */
    clt_qp[i] = NULL;
    while (clt_qp[i] == NULL) {
      clt_qp[i] = hrd_get_published_qp(clt_name);
      if (clt_qp[i] == NULL) {
        usleep(200000);
      }
    }

    struct ibv_ah_attr ah_attr = {
        .is_global = 0,
        .dlid = clt_qp[i]->lid,
        .sl = 0,
        .src_path_bits = 0,
        /* port_num (> 1): device-local port for responses to this client */
        .port_num = local_port_i + 1,
    };

    ah[i] = ibv_create_ah(cb[cb_i]->pd, &ah_attr);
    assert(ah[i] != NULL);
  }


    while (1) {
      sleep(100);


    }

  return NULL;
}
