#ifndef FERRUM_BRIDGE_H
#define FERRUM_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
  uint8_t *data;
  size_t len;
} BridgeBuffer;

void *bridge_create(void *env, void *dispatcher, BridgeBuffer *error);
void *bridge_open(void *bridge, const uint8_t *method, size_t method_len,
    const uint8_t *metadata, size_t metadata_len, int64_t timeout_ns, BridgeBuffer *error);
int bridge_send(void *call, const uint8_t *data, size_t len, BridgeBuffer *error);
int bridge_half_close(void *call, BridgeBuffer *error);
int bridge_poll(void *call, int64_t timeout_ms, BridgeBuffer *out, BridgeBuffer *error);
void bridge_cancel(void *call);
void bridge_finish(void *call);
void bridge_call_free(void *call);
void bridge_free(void *bridge);
void bridge_buffer_free(BridgeBuffer *buffer);

int64_t bridge_native_start(void *env, void *clazz, void *address, void *dispatcher);
void *bridge_native_address(void *env, void *clazz, int64_t id);
void bridge_native_stop(void *env, void *clazz, int64_t id);

/* Typed query engine ABI; these handles never carry service RPC envelopes. */
void *eng_task_new(void *engine, BridgeBuffer *error);
void *eng_open_session(void *engine, const uint8_t *user, size_t user_len,
    const uint8_t *id, size_t id_len, void *task, BridgeBuffer *error);
void *eng_prepare(void *session, const uint8_t *relation, size_t len,
    void *task, BridgeBuffer *error);
int eng_schema(void *query, BridgeBuffer *out, BridgeBuffer *error);
int eng_next(void *query, BridgeBuffer *out, BridgeBuffer *error);

/* Arrow C Data Interface ABI. These descriptors never own/copy value buffers. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
};
struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
};
#endif

/* Only success (1) transfers lease to the exported array. EOF/error leave it with caller.
 * defer_release runs on the final consumer thread and must queue owner disposal.
 * eng_arrow_owner_free closes Java buffers and is called by that queue's JNI worker. */
int eng_next_arrow(void *query, struct ArrowArray *out, void *lease,
    void (*defer_release)(void *lease, void *owner), BridgeBuffer *error);
int64_t eng_arrow_buffer_size(const struct ArrowArray *array, int64_t index);
void eng_arrow_owner_free(void *owner);
int eng_flags(void *query, int32_t *flags, BridgeBuffer *error);
int eng_config(void *session, int32_t opcode, const uint8_t *fields, size_t len,
    void *task, BridgeBuffer *out, BridgeBuffer *error);
void eng_cancel(void *task);
void eng_free(void *handle);

int64_t eng_native_start(void *env, void *clazz, void *address, void *factory);
void *eng_native_address(void *env, void *clazz, int64_t id);
void eng_native_stop(void *env, void *clazz, int64_t id);

#endif
