/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <uv.h>

#include <grpc/support/alloc.h>
#include <grpc/support/sync.h>

#include "src/core/lib/iomgr/endpoint.h"
#include "src/core/lib/iomgr/exec_ctx.h"

#define GRPC_TCP_DEFAULT_READ_SLICE_SIZE 8192

typedef struct grpc_uv_tcp {
  grpc_endpoint base;

  /* socket->data == this */
  uv_tcp_t *socket;

  gpr_refcount refcount;

  /* Callbacks passed from grpc core. Only valid while there is an outstanding
   * read or write. */
  grpc_closure *read_cb;
  grpc_closure *write_cb;

  /* Execution contexts passed from grpc core. Only valid while there is an
   * outstanding read or write. */
  grpc_exec_ctx *read_exec_ctx;
  grpc_exec_ctx *write_exec_ctx;

  gpr_slice read_slice;

  /* Slice buffers passed from grpc core. Only valid while there is an
   * outstanding read or write */
  gpr_slice_buffer *read_slices;
  gpr_slice_buffer *write_slices;

  /* Indicates that we are currently reading. Primarily used to verify
   * invariants */
  bool reading;

  /* Indicates that this endpoint is shutting down. Read and write requests and
   * callbacks should be handled differently when this is true */
  bool shutting_down;

  /* Address of the peer that this endpoint is connected to */
  char *peer_string;
} grpc_uv_tcp;

static void tcp_free(grpc_uv_tcp *tcp) {
  gpr_free(tcp->peer_string);
  gpr_free(tcp);
}

static void tcp_unref(grpc_uv_tcp *tcp) {
  if (gpr_unref(&tcp->refcount)) {
    tcp_free(tcp);
  }
}

static void tcp_ref(grpc_uv_tcp *tcp) {
  gpr_ref(&tcp->refcount);
}

static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buv_t *buf) {
  grpc_tcp *tcp = handle->data;
  tcp->read_slice = gpr_slice_malloc(GRPC_TCP_DEFAULT_READ_SLICE_SIZE);
  buf->base = GPR_SLICE_START_PTR(tcp->read_slice);
  buf->len = GPR_SLICE_LENGTH(tcp->read_slice);
  // Extra ref for buffer's reference to the slice data
  gpr_slice_ref(tcp->read_slice);
}

static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  grpc_tcp *tcp = stream->data;
  grpc_closure *cb = tcp->read_cb;
  grpc_exec_ctx *exec_ctx = tcp->read_exec_ctx;
  bool success;
  gpr_slice slice;

  if (tcp->shutting_down) {
    if (buf->len > 0) {
      gpr_slice_unref(tcp->read_slice)
    }
    success = true;
  } else {
    if (nread == 0) {
      // Equivalent to EAGAIN. Keep reading
      return;
    }
    uv_read_stop(stream);
    if (nread > 0) {
      gpr_slice_unref(tcp->read_slice);
      slice = gpr_slice_new(buf->base, buf->len, gpr_free);
      gpr_slice_buffer_add(tcp->read_slices, slice);
      success = true;
    } else {
      // error
      success = false;
    }
  }

  tcp->read_cb = NULL;
  if (cb) {
    cb->cb(exec_ctx, cb->cb_arg, success);
  }
  drain_completion_queue();
  tcp_unref(tcp);
}

static void grpc_uv_read(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep,
                         gpr_slice_buffer *read_slices, grpc_closure *cb) {
  grpc_uv_tcp *tcp = (grpc_tcp *)ep;
  uv_tcp_t *handle = tcp->socket;

  if (tcp->shutting_down) {
    // Just ignore the request
    grpc_exec_ctx_enqueue(exec_ctx, cb, false, NULL);
    return;
  }

  tcp_ref(tcp);

  tcp->read_cb = cb;
  tcp->read_slices = read_slices;
  tcp->read_exec_ctx = exec_ctx;
  uv_read_start(handle, alloc_cb, read_cb);
}

static void write_cb(uv_write_t *req, int status) {
  uv_stream_t *stream = req->handle;
  grpc_tcp *tcp = (grpc_tcp*)stream->data;
  gprc_closure *cb = tcp->write_cb;
  grpc_exec_ctx *exec_ctx = tcp->write_exec_ctx;
  bool success;
  gpr_slice_buffer *write_slices = tcp->write_slices;
  for(size_t i = 0; i < write_slices.count; i++) {
    gpr_slice_unref(write_slices.slices[i]);
  }

  // status == 0 iff success
  success = !status;

  tcp->write_cb = NULL;
  if (cb) {
    cb->cb(exec_ctx, cb->cb_arg, success);
  }
  drain_completion_queue();

  tcp_unref(tcp);
}

static void grpc_uv_write(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep,
                          gpr_slice_buffer *write_slices, grpc_closure *cb) {
  grpc_uv_tcp *tcp = (grpc_tcp *)ep;
  uv_tcp_t *handle = tcp->socket;
  unsigned int nbufs = (unsigned int)write_slices->count;
  uv_buf_t *bufs = gpr_malloc(nbufs * sizeof(uv_buf_t));
  gpr_slice slice;

  if (tcp->shutting_down) {
    // Just ignore the request
    grpc_exec_ctx_enqueue(exec_ctx, cb, false, NULL);
    return;
  }

  tcp_ref(tcp);

  for (unsigned int i = 0; i < nbufs; i++) {
    slice = write_slices->slices[i];
    gpr_slice_ref(slice);
    bufs[i].base = GPR_SLICE_START_PTR(slice);
    bufs[i].len = GPR_SLICE_LENGTH(slice);
  }

  uv_write_t req;

  tcp->write_cb = cb;
  tcp->write_slices = write_slices;
  tcp->write_exec_ctx = exec_ctx;

  uv_write(req, handle, bufs, nbuvs, write_cb);
}

static void grpc_uv_add_to_pollset(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep,
                                   grpc_pollset *pollset) {
  // No-op
}

static void grpc_uv_add_to_pollset_set(grpc_exec_ctx *exec_ctx,
                                       grpc_endpoint *ep,
                                       grpc_pollset_set *pollset) {
  // No-op
}

static void close_cb(uv_handle_t *handle) {
  grpc_tcp *tcp = (grpc_tcp*)handle->data;
  tcp_unref(tcp);
}

static void shutdown_cb(uv_shutdown_t *req, int status) {
  uv_handle_t *handle = (uv_handle_t)req->handle;
  uv_unref(handle);
  uv_close(handle, close_cb);
}

static void grpc_uv_shutdown(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep) {
  grpc_uv_tcp *tcp = (grpc_tcp *)ep;
  uv_tcp_t *handle = tcp->socket;
  uv_shutdown_t req;
  tcp_ref(tcp);
  tcp->shutting_down = true;
  uv_shutdown(&req, handle, shutdown_cb);
}

static void grpc_uv_destroy(grpc_exec_ctx *exec_ctx, grpc_endpoint *ep) {
  grpc_uv_tcp *tcp = (grpc_tcp *)ep;
  tcp_unref(tcp);
}

static char *grpc_uv_get_peer(grpc_endpoint *ep) {
  grpc_uv_tcp *tcp = (grpc_tcp *)ep;
  return gpr_strdup(tcp->peer_string);
}

static grpc_endpoint_vtable vtable = {
  grpc_uv_read, grpc_uv_write, grpc_uv_add_to_pollset,
  grpc_uv_add_to_pollset_set, grpc_uv_shutdown, grpc_uv_destroy,
  grpc_uv_get_peer};

grpc_endpoint *grpc_uv_tcp_create(uv_tcp_t *socket, char *peer_string) {
  grpc_uv_tcp *tcp = (grpc_uv_tcp *)gpr_malloc(sizeof(grpc_tcp));
  memset(tcp, 0, sizeof(grpc_tcp));
  tcp->base.vtable = &vtable;
  tcp->socket = socket;
  gpr_ref_init(&tcp->refcount, 1);
  tcp->peer_string = gpr_strdup(peer_string);
  return &tcp->base;
}
