#include "bridge.h"

#include <jni.h>
#include <inttypes.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  JavaVM *vm;
  jobject loader;
  jclass thread_class;
  jmethodID current_thread;
  jmethodID get_loader;
  jmethodID set_loader;
  atomic_uint references;
} JavaContext;

typedef struct {
  JavaContext *context;
  jobject dispatcher;
  jmethodID open;
  jmethodID send;
  jmethodID half_close;
  jmethodID poll;
  jmethodID cancel;
  jmethodID finish;
} Bridge;

typedef struct {
  JavaContext *context;
  jobject call;
  jmethodID send;
  jmethodID half_close;
  jmethodID poll;
  jmethodID cancel;
  jmethodID finish;
} Call;

typedef struct {
  JavaContext *context;
  JNIEnv *env;
  jobject thread;
  jobject previous_loader;
  int attached;
  int loader_changed;
} JavaScope;

extern int64_t ferrum_start(const uint8_t *, size_t, void *, BridgeBuffer *);
extern int64_t ferrum_engine_start(const uint8_t *, size_t, void *, BridgeBuffer *);
extern int ferrum_address(int64_t, BridgeBuffer *, BridgeBuffer *);
extern int ferrum_stop(int64_t, BridgeBuffer *);

static void clear_buffer(BridgeBuffer *buffer) {
  if (buffer != NULL) {
    buffer->data = NULL;
    buffer->len = 0;
  }
}

static void set_error(BridgeBuffer *error, const char *message) {
  if (error == NULL || error->data != NULL) {
    return;
  }
  size_t len = strlen(message);
  uint8_t *data = malloc(len == 0 ? 1 : len);
  if (data != NULL) {
    memcpy(data, message, len);
    error->data = data;
    error->len = len;
  }
}

void bridge_buffer_free(BridgeBuffer *buffer) {
  if (buffer != NULL) {
    free(buffer->data);
    clear_buffer(buffer);
  }
}

/* GetStringUTFChars uses modified UTF-8, so encode UTF-16 explicitly instead. */
static int string_buffer(JNIEnv *env, jstring value, BridgeBuffer *out) {
  jsize len = (*env)->GetStringLength(env, value);
  const jchar *chars = (*env)->GetStringChars(env, value, NULL);
  if (chars == NULL) {
    return -1;
  }
  uint8_t *data = malloc((size_t) len * 3 + 1);
  if (data == NULL) {
    (*env)->ReleaseStringChars(env, value, chars);
    return -1;
  }
  size_t written = 0;
  for (jsize i = 0; i < len; i++) {
    uint32_t code = chars[i];
    if (code >= 0xd800 && code <= 0xdbff && i + 1 < len
        && chars[i + 1] >= 0xdc00 && chars[i + 1] <= 0xdfff) {
      code = 0x10000 + ((code - 0xd800) << 10) + chars[++i] - 0xdc00;
    } else if (code >= 0xd800 && code <= 0xdfff) {
      code = 0xfffd;
    }
    if (code < 0x80) {
      data[written++] = (uint8_t) code;
    } else if (code < 0x800) {
      data[written++] = (uint8_t) (0xc0 | (code >> 6));
      data[written++] = (uint8_t) (0x80 | (code & 0x3f));
    } else if (code < 0x10000) {
      data[written++] = (uint8_t) (0xe0 | (code >> 12));
      data[written++] = (uint8_t) (0x80 | ((code >> 6) & 0x3f));
      data[written++] = (uint8_t) (0x80 | (code & 0x3f));
    } else {
      data[written++] = (uint8_t) (0xf0 | (code >> 18));
      data[written++] = (uint8_t) (0x80 | ((code >> 12) & 0x3f));
      data[written++] = (uint8_t) (0x80 | ((code >> 6) & 0x3f));
      data[written++] = (uint8_t) (0x80 | (code & 0x3f));
    }
  }
  (*env)->ReleaseStringChars(env, value, chars);
  out->data = data;
  out->len = written;
  return 0;
}

static int exception_error(JNIEnv *env, BridgeBuffer *error) {
  if (!(*env)->ExceptionCheck(env)) {
    return 0;
  }
  jthrowable throwable = (*env)->ExceptionOccurred(env);
  (*env)->ExceptionClear(env);
  jclass type = throwable == NULL ? NULL : (*env)->GetObjectClass(env, throwable);
  jmethodID to_string = type == NULL ? NULL
      : (*env)->GetMethodID(env, type, "toString", "()Ljava/lang/String;");
  jstring description = to_string == NULL ? NULL
      : (*env)->CallObjectMethod(env, throwable, to_string);
  if (!(*env)->ExceptionCheck(env) && description != NULL && error != NULL
      && error->data == NULL) {
    (void) string_buffer(env, description, error);
  }
  if ((*env)->ExceptionCheck(env)) {
    (*env)->ExceptionClear(env);
  }
  if (description != NULL) (*env)->DeleteLocalRef(env, description);
  if (type != NULL) (*env)->DeleteLocalRef(env, type);
  if (throwable != NULL) (*env)->DeleteLocalRef(env, throwable);
  set_error(error, "Java bridge operation failed");
  return -1;
}

static void scope_leave(JavaScope *scope) {
  if (scope->env == NULL) return;
  JNIEnv *env = scope->env;
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  if (scope->loader_changed) {
    (*env)->CallVoidMethod(env, scope->thread, scope->context->set_loader,
        scope->previous_loader);
    if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  }
  if (scope->previous_loader != NULL) (*env)->DeleteLocalRef(env, scope->previous_loader);
  if (scope->thread != NULL) (*env)->DeleteLocalRef(env, scope->thread);
  if (scope->attached) (*scope->context->vm)->DetachCurrentThread(scope->context->vm);
  scope->env = NULL;
}

static int scope_enter(JavaContext *context, JavaScope *scope, BridgeBuffer *error) {
  memset(scope, 0, sizeof(*scope));
  scope->context = context;
  jint state = (*context->vm)->GetEnv(context->vm, (void **) &scope->env, JNI_VERSION_1_8);
  if (state == JNI_EDETACHED) {
    if ((*context->vm)->AttachCurrentThreadAsDaemon(
        context->vm, (void **) &scope->env, NULL) != JNI_OK) {
      scope->env = NULL;
      set_error(error, "cannot attach Rust worker to JVM");
      return -1;
    }
    scope->attached = 1;
  } else if (state != JNI_OK) {
    scope->env = NULL;
    set_error(error, "cannot access JVM from Rust worker");
    return -1;
  }
  JNIEnv *env = scope->env;
  scope->thread = (*env)->CallStaticObjectMethod(env, context->thread_class,
      context->current_thread);
  if (exception_error(env, error) != 0 || scope->thread == NULL) goto failed;
  scope->previous_loader = (*env)->CallObjectMethod(env, scope->thread, context->get_loader);
  if (exception_error(env, error) != 0) goto failed;
  if (!(*env)->IsSameObject(env, scope->previous_loader, context->loader)) {
    (*env)->CallVoidMethod(env, scope->thread, context->set_loader, context->loader);
    if (exception_error(env, error) != 0) goto failed;
    scope->loader_changed = 1;
  }
  return 0;
failed:
  set_error(error, "cannot prepare JVM worker context");
  scope_leave(scope);
  return -1;
}

static void context_release(JavaContext *context) {
  if (atomic_fetch_sub_explicit(&context->references, 1, memory_order_acq_rel) != 1) return;
  JavaScope scope;
  if (scope_enter(context, &scope, NULL) == 0) {
    JNIEnv *env = scope.env;
    /* Restore before deleting the globals scope_leave uses. */
    if (scope.loader_changed) {
      (*env)->CallVoidMethod(env, scope.thread, context->set_loader, scope.previous_loader);
      if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
      scope.loader_changed = 0;
    }
    if (context->loader != NULL) (*env)->DeleteGlobalRef(env, context->loader);
    (*env)->DeleteGlobalRef(env, context->thread_class);
    scope_leave(&scope);
  }
  free(context);
}

static jbyteArray byte_array(JNIEnv *env, const uint8_t *data, size_t len,
    BridgeBuffer *error) {
  if (len > INT_MAX || (len != 0 && data == NULL)) {
    set_error(error, "Java bridge byte array is too large or invalid");
    return NULL;
  }
  jbyteArray array = (*env)->NewByteArray(env, (jsize) len);
  if (exception_error(env, error) != 0 || array == NULL) return NULL;
  if (len != 0) (*env)->SetByteArrayRegion(env, array, 0, (jsize) len, (const jbyte *) data);
  if (exception_error(env, error) != 0) {
    (*env)->DeleteLocalRef(env, array);
    return NULL;
  }
  return array;
}

static jstring utf8_string(JNIEnv *env, const uint8_t *data, size_t len,
    BridgeBuffer *error) {
  jbyteArray bytes = byte_array(env, data, len, error);
  if (bytes == NULL) return NULL;
  jclass type = (*env)->FindClass(env, "java/lang/String");
  jmethodID constructor = type == NULL ? NULL
      : (*env)->GetMethodID(env, type, "<init>", "([BLjava/lang/String;)V");
  jstring encoding = constructor == NULL ? NULL : (*env)->NewStringUTF(env, "UTF-8");
  jstring value = NULL;
  if (exception_error(env, error) == 0 && encoding != NULL) {
    value = (*env)->NewObject(env, type, constructor, bytes, encoding);
    if (exception_error(env, error) != 0) value = NULL;
  }
  if (encoding != NULL) (*env)->DeleteLocalRef(env, encoding);
  if (type != NULL) (*env)->DeleteLocalRef(env, type);
  (*env)->DeleteLocalRef(env, bytes);
  return value;
}

static int array_buffer(JNIEnv *env, jbyteArray array, BridgeBuffer *out,
    BridgeBuffer *error) {
  jsize len = (*env)->GetArrayLength(env, array);
  if (exception_error(env, error) != 0) return -1;
  uint8_t *data = malloc(len == 0 ? 1 : (size_t) len);
  if (data == NULL) {
    set_error(error, "cannot allocate Java bridge buffer");
    return -1;
  }
  if (len != 0) (*env)->GetByteArrayRegion(env, array, 0, len, (jbyte *) data);
  if (exception_error(env, error) != 0) {
    free(data);
    return -1;
  }
  out->data = data;
  out->len = (size_t) len;
  return 0;
}

void *bridge_create(void *raw_env, void *dispatcher, BridgeBuffer *error) {
  clear_buffer(error);
  JNIEnv *env = raw_env;
  Bridge *bridge = calloc(1, sizeof(*bridge));
  JavaContext *context = calloc(1, sizeof(*context));
  if (bridge == NULL || context == NULL) {
    free(bridge);
    free(context);
    set_error(error, "cannot allocate JVM bridge");
    return NULL;
  }
  bridge->context = context;
  atomic_init(&context->references, 1);
  if ((*env)->GetJavaVM(env, &context->vm) != JNI_OK) goto failed;
  jclass thread = (*env)->FindClass(env, "java/lang/Thread");
  if (exception_error(env, error) != 0 || thread == NULL) goto failed;
  context->thread_class = (*env)->NewGlobalRef(env, thread);
  (*env)->DeleteLocalRef(env, thread);
  if (exception_error(env, error) != 0 || context->thread_class == NULL) goto failed;
  context->current_thread = (*env)->GetStaticMethodID(env, context->thread_class,
      "currentThread", "()Ljava/lang/Thread;");
  if (exception_error(env, error) != 0 || context->current_thread == NULL) goto failed;
  context->get_loader = (*env)->GetMethodID(env, context->thread_class,
      "getContextClassLoader", "()Ljava/lang/ClassLoader;");
  if (exception_error(env, error) != 0 || context->get_loader == NULL) goto failed;
  context->set_loader = (*env)->GetMethodID(env, context->thread_class,
      "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
  if (exception_error(env, error) != 0 || context->set_loader == NULL) goto failed;
  jobject current = (*env)->CallStaticObjectMethod(env, context->thread_class,
      context->current_thread);
  if (exception_error(env, error) != 0 || current == NULL) goto failed;
  jobject loader = (*env)->CallObjectMethod(env, current, context->get_loader);
  (*env)->DeleteLocalRef(env, current);
  if (exception_error(env, error) != 0) goto failed;
  if (loader != NULL) {
    context->loader = (*env)->NewGlobalRef(env, loader);
    (*env)->DeleteLocalRef(env, loader);
    if (exception_error(env, error) != 0 || context->loader == NULL) goto failed;
  }
  bridge->dispatcher = (*env)->NewGlobalRef(env, dispatcher);
  if (exception_error(env, error) != 0 || bridge->dispatcher == NULL) goto failed;
  jclass type = (*env)->GetObjectClass(env, dispatcher);
  if (exception_error(env, error) != 0 || type == NULL) goto failed;
  bridge->open = (*env)->GetMethodID(env, type, "open",
      "(Ljava/lang/String;[[BJ)Lorg/apache/spark/sql/connect/nativebridge/SparkConnectBridge$Call;");
  (*env)->DeleteLocalRef(env, type);
  if (exception_error(env, error) != 0 || bridge->open == NULL) goto failed;
  jclass call_type = (*env)->FindClass(env,
      "org/apache/spark/sql/connect/nativebridge/SparkConnectBridge$Call");
  if (exception_error(env, error) != 0 || call_type == NULL) goto failed;
  bridge->send = (*env)->GetMethodID(env, call_type, "send", "([B)Z");
  if (exception_error(env, error) == 0)
    bridge->half_close = (*env)->GetMethodID(env, call_type, "halfClose", "()V");
  if (exception_error(env, error) == 0)
    bridge->poll = (*env)->GetMethodID(env, call_type, "poll", "(J)[B");
  if (exception_error(env, error) == 0)
    bridge->cancel = (*env)->GetMethodID(env, call_type, "cancel", "()V");
  if (exception_error(env, error) == 0)
    bridge->finish = (*env)->GetMethodID(env, call_type, "finish", "()V");
  (void) exception_error(env, error);
  (*env)->DeleteLocalRef(env, call_type);
  if (bridge->send == NULL || bridge->half_close == NULL || bridge->poll == NULL
      || bridge->cancel == NULL || bridge->finish == NULL) goto failed;
  return bridge;
failed:
  set_error(error, "cannot initialize JVM bridge");
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  if (bridge->dispatcher != NULL) (*env)->DeleteGlobalRef(env, bridge->dispatcher);
  if (context->loader != NULL) (*env)->DeleteGlobalRef(env, context->loader);
  if (context->thread_class != NULL) (*env)->DeleteGlobalRef(env, context->thread_class);
  free(context);
  free(bridge);
  return NULL;
}

static int read_u32(const uint8_t *data, size_t len, size_t *position, uint32_t *out) {
  if (*position > len || len - *position < 4) return -1;
  const uint8_t *value = data + *position;
  *out = ((uint32_t) value[0] << 24) | ((uint32_t) value[1] << 16)
      | ((uint32_t) value[2] << 8) | value[3];
  *position += 4;
  return 0;
}

static jobjectArray metadata_array(JNIEnv *env, const uint8_t *data, size_t len,
    BridgeBuffer *error) {
  size_t position = 0;
  uint32_t count;
  if (data == NULL || read_u32(data, len, &position, &count) != 0
      || count > INT_MAX / 2 || count > (len - position) / 8) {
    set_error(error, "invalid Java bridge metadata encoding");
    return NULL;
  }
  jclass bytes_type = (*env)->FindClass(env, "[B");
  if (exception_error(env, error) != 0 || bytes_type == NULL) return NULL;
  jobjectArray array = (*env)->NewObjectArray(env, (jsize) (count * 2), bytes_type, NULL);
  (*env)->DeleteLocalRef(env, bytes_type);
  if (exception_error(env, error) != 0 || array == NULL) return NULL;
  for (uint32_t i = 0; i < count * 2; i++) {
    uint32_t value_len;
    if (read_u32(data, len, &position, &value_len) != 0 || value_len > len - position) {
      set_error(error, "invalid Java bridge metadata encoding");
      (*env)->DeleteLocalRef(env, array);
      return NULL;
    }
    jbyteArray value = byte_array(env, data + position, value_len, error);
    if (value == NULL) {
      (*env)->DeleteLocalRef(env, array);
      return NULL;
    }
    (*env)->SetObjectArrayElement(env, array, (jsize) i, value);
    (*env)->DeleteLocalRef(env, value);
    if (exception_error(env, error) != 0) {
      (*env)->DeleteLocalRef(env, array);
      return NULL;
    }
    position += value_len;
  }
  if (position != len) {
    set_error(error, "trailing Java bridge metadata bytes");
    (*env)->DeleteLocalRef(env, array);
    return NULL;
  }
  return array;
}

void *bridge_open(void *raw_bridge, const uint8_t *method, size_t method_len,
    const uint8_t *metadata, size_t metadata_len, int64_t timeout_ns, BridgeBuffer *error) {
  clear_buffer(error);
  Bridge *bridge = raw_bridge;
  JavaScope scope;
  if (scope_enter(bridge->context, &scope, error) != 0) return NULL;
  JNIEnv *env = scope.env;
  Call *call = calloc(1, sizeof(*call));
  if (call == NULL) {
    set_error(error, "cannot allocate JVM call");
    scope_leave(&scope);
    return NULL;
  }
  jstring method_name = utf8_string(env, method, method_len, error);
  jobjectArray headers = method_name == NULL ? NULL
      : metadata_array(env, metadata, metadata_len, error);
  jobject object = headers == NULL ? NULL : (*env)->CallObjectMethod(env,
      bridge->dispatcher, bridge->open, method_name, headers, (jlong) timeout_ns);
  if (exception_error(env, error) == 0 && object != NULL) {
    call->context = bridge->context;
    call->call = (*env)->NewGlobalRef(env, object);
    if (exception_error(env, error) != 0 || call->call == NULL) {
      (*env)->CallVoidMethod(env, object, bridge->finish);
      (void) exception_error(env, error);
      if (call->call != NULL) (*env)->DeleteGlobalRef(env, call->call);
      call->call = NULL;
    } else {
      call->send = bridge->send;
      call->half_close = bridge->half_close;
      call->poll = bridge->poll;
      call->cancel = bridge->cancel;
      call->finish = bridge->finish;
      atomic_fetch_add_explicit(&call->context->references, 1, memory_order_relaxed);
    }
  }
  if (call->call == NULL) {
    free(call);
    call = NULL;
    set_error(error, "cannot open JVM call");
  }
  if (object != NULL) (*env)->DeleteLocalRef(env, object);
  if (headers != NULL) (*env)->DeleteLocalRef(env, headers);
  if (method_name != NULL) (*env)->DeleteLocalRef(env, method_name);
  scope_leave(&scope);
  return call;
}

int bridge_send(void *raw_call, const uint8_t *data, size_t len, BridgeBuffer *error) {
  clear_buffer(error);
  Call *call = raw_call;
  JavaScope scope;
  if (scope_enter(call->context, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jbyteArray message = byte_array(env, data, len, error);
  int result = -1;
  if (message != NULL) {
    jboolean accepted = (*env)->CallBooleanMethod(env, call->call, call->send, message);
    if (exception_error(env, error) == 0) result = accepted ? 1 : 0;
    (*env)->DeleteLocalRef(env, message);
  }
  scope_leave(&scope);
  return result;
}

static int invoke_void(Call *call, jmethodID method, BridgeBuffer *error) {
  clear_buffer(error);
  JavaScope scope;
  if (scope_enter(call->context, &scope, error) != 0) return -1;
  (*scope.env)->CallVoidMethod(scope.env, call->call, method);
  int result = exception_error(scope.env, error);
  scope_leave(&scope);
  return result;
}

int bridge_half_close(void *raw_call, BridgeBuffer *error) {
  Call *call = raw_call;
  return invoke_void(call, call->half_close, error);
}

int bridge_poll(void *raw_call, int64_t timeout_ms, BridgeBuffer *out, BridgeBuffer *error) {
  clear_buffer(out);
  clear_buffer(error);
  Call *call = raw_call;
  JavaScope scope;
  if (scope_enter(call->context, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jbyteArray event = (*env)->CallObjectMethod(env, call->call, call->poll, (jlong) timeout_ms);
  int result = exception_error(env, error);
  if (result == 0 && event != NULL) {
    result = array_buffer(env, event, out, error) == 0 ? 1 : -1;
  }
  if (event != NULL) (*env)->DeleteLocalRef(env, event);
  scope_leave(&scope);
  return result;
}

void bridge_cancel(void *raw_call) {
  Call *call = raw_call;
  (void) invoke_void(call, call->cancel, NULL);
}

void bridge_finish(void *raw_call) {
  Call *call = raw_call;
  (void) invoke_void(call, call->finish, NULL);
}

void bridge_call_free(void *raw_call) {
  Call *call = raw_call;
  if (call == NULL) return;
  JavaScope scope;
  if (scope_enter(call->context, &scope, NULL) == 0) {
    (*scope.env)->DeleteGlobalRef(scope.env, call->call);
    scope_leave(&scope);
  }
  context_release(call->context);
  free(call);
}

void bridge_free(void *raw_bridge) {
  Bridge *bridge = raw_bridge;
  if (bridge == NULL) return;
  JavaScope scope;
  if (scope_enter(bridge->context, &scope, NULL) == 0) {
    (*scope.env)->DeleteGlobalRef(scope.env, bridge->dispatcher);
    scope_leave(&scope);
  }
  context_release(bridge->context);
  free(bridge);
}

static void throw_error(JNIEnv *env, BridgeBuffer *error) {
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  if (error->data == NULL) set_error(error, "native Spark Connect operation failed");
  jstring message = utf8_string(env, error->data, error->len, NULL);
  jclass type = (*env)->FindClass(env, "java/lang/IllegalStateException");
  if (!(*env)->ExceptionCheck(env) && message != NULL && type != NULL) {
    jmethodID constructor = (*env)->GetMethodID(env, type, "<init>", "(Ljava/lang/String;)V");
    if (!(*env)->ExceptionCheck(env) && constructor != NULL) {
      jthrowable throwable = (*env)->NewObject(env, type, constructor, message);
      if (!(*env)->ExceptionCheck(env) && throwable != NULL) {
        (*env)->Throw(env, throwable);
        (*env)->DeleteLocalRef(env, throwable);
      }
    }
  }
  if (message != NULL) (*env)->DeleteLocalRef(env, message);
  if (type != NULL) (*env)->DeleteLocalRef(env, type);
  bridge_buffer_free(error);
}

int64_t bridge_native_start(void *raw_env, void *clazz, void *raw_address, void *dispatcher) {
  (void) clazz;
  JNIEnv *env = raw_env;
  BridgeBuffer error = {0};
  BridgeBuffer address = {0};
  if (raw_address == NULL || dispatcher == NULL) {
    set_error(&error, "native address and dispatcher must be non-null");
    throw_error(env, &error);
    return 0;
  }
  if (array_buffer(env, raw_address, &address, &error) != 0) {
    throw_error(env, &error);
    return 0;
  }
  void *bridge = bridge_create(env, dispatcher, &error);
  if (bridge == NULL) {
    bridge_buffer_free(&address);
    throw_error(env, &error);
    return 0;
  }
  /* ferrum_start owns bridge on both success and failure. */
  int64_t id = ferrum_start(address.data, address.len, bridge, &error);
  bridge_buffer_free(&address);
  if (id == 0) throw_error(env, &error);
  else bridge_buffer_free(&error);
  return id;
}

void *bridge_native_address(void *raw_env, void *clazz, int64_t id) {
  (void) clazz;
  JNIEnv *env = raw_env;
  BridgeBuffer address = {0};
  BridgeBuffer error = {0};
  if (ferrum_address(id, &address, &error) != 0) {
    bridge_buffer_free(&address);
    throw_error(env, &error);
    return NULL;
  }
  jbyteArray result = byte_array(env, address.data, address.len, &error);
  bridge_buffer_free(&address);
  if (result == NULL) throw_error(env, &error);
  else bridge_buffer_free(&error);
  return result;
}

void bridge_native_stop(void *raw_env, void *clazz, int64_t id) {
  (void) clazz;
  BridgeBuffer error = {0};
  if (ferrum_stop(id, &error) != 0) throw_error(raw_env, &error);
  else bridge_buffer_free(&error);
}

/* The engine ABI is separate from the legacy handler-dispatch bridge above. */
typedef enum {
  ENGINE_FACTORY,
  ENGINE_TASK,
  ENGINE_SESSION,
  ENGINE_QUERY,
  ENGINE_BATCH,
  ENGINE_EXCEPTION,
  ENGINE_NODE,
  ENGINE_CLASS_COUNT
} EngineKind;

typedef struct {
  JavaContext *context;
  atomic_uint references;
  jclass classes[ENGINE_CLASS_COUNT];
  jmethodID close[ENGINE_EXCEPTION];
  jmethodID new_task;
  jmethodID open_session;
  jmethodID prepare;
  jmethodID config;
  jmethodID schema;
  jmethodID next;
  jmethodID next_arrow;
  jmethodID descriptor;
  jmethodID node_length;
  jmethodID node_null_count;
  jmethodID node_offset;
  jmethodID node_buffers;
  jmethodID node_buffer_sizes;
  jmethodID node_children;
  jmethodID local;
  jmethodID streaming;
  jmethodID cancel;
  jmethodID exception_code;
} EngineApi;

typedef struct {
  EngineApi *api;
  jobject object;
  EngineKind kind;
} EngineHandle;

static void engine_error_bytes(BridgeBuffer *error, int code,
    const uint8_t *message, size_t len) {
  if (error == NULL || error->data != NULL) return;
  char prefix[40];
  int prefix_len = snprintf(prefix, sizeof(prefix), "JVM_ENGINE_ERROR:%d:", code);
  if (prefix_len < 0 || (size_t) prefix_len >= sizeof(prefix)
      || len > SIZE_MAX - (size_t) prefix_len) return;
  uint8_t *data = malloc((size_t) prefix_len + len);
  if (data == NULL) return;
  memcpy(data, prefix, (size_t) prefix_len);
  if (len != 0) memcpy(data + prefix_len, message, len);
  error->data = data;
  error->len = (size_t) prefix_len + len;
}

static void engine_error(BridgeBuffer *error, int code, const char *message) {
  engine_error_bytes(error, code, (const uint8_t *) message, strlen(message));
}

/* Shared low-level helpers report plain text. Frame it at this ABI's boundary. */
static void engine_normalize_error(BridgeBuffer *error, const char *fallback) {
  static const char prefix[] = "JVM_ENGINE_ERROR:";
  if (error == NULL) return;
  if (error->len >= sizeof(prefix) - 1 && error->data != NULL
      && memcmp(error->data, prefix, sizeof(prefix) - 1) == 0) return;
  BridgeBuffer previous = *error;
  clear_buffer(error);
  if (previous.data != NULL) {
    engine_error_bytes(error, 13, previous.data, previous.len);
    bridge_buffer_free(&previous);
  } else {
    engine_error(error, 13, fallback);
  }
}

static int engine_exception(JNIEnv *env, EngineApi *api, BridgeBuffer *error) {
  if (!(*env)->ExceptionCheck(env)) return 0;
  jthrowable throwable = (*env)->ExceptionOccurred(env);
  (*env)->ExceptionClear(env);
  int code = 13;
  int typed = throwable != NULL && api != NULL
      && api->classes[ENGINE_EXCEPTION] != NULL && api->exception_code != NULL
      && (*env)->IsInstanceOf(env, throwable, api->classes[ENGINE_EXCEPTION]);
  if (typed) {
    jint reported = (*env)->CallIntMethod(env, throwable, api->exception_code);
    if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
    else if (reported >= 1 && reported <= 16) code = reported;
  }
  jclass type = throwable == NULL ? NULL : (*env)->GetObjectClass(env, throwable);
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  jmethodID describe = type == NULL ? NULL : (*env)->GetMethodID(env, type,
      typed ? "getMessage" : "toString", "()Ljava/lang/String;");
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  jstring description = describe == NULL ? NULL
      : (*env)->CallObjectMethod(env, throwable, describe);
  if ((*env)->ExceptionCheck(env)) {
    (*env)->ExceptionClear(env);
    description = NULL;
  }
  BridgeBuffer message = {0};
  if (description != NULL) (void) string_buffer(env, description, &message);
  if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
  if (message.data != NULL) engine_error_bytes(error, code, message.data, message.len);
  else engine_error(error, code, "Java engine operation failed");
  bridge_buffer_free(&message);
  if (description != NULL) (*env)->DeleteLocalRef(env, description);
  if (type != NULL) (*env)->DeleteLocalRef(env, type);
  if (throwable != NULL) (*env)->DeleteLocalRef(env, throwable);
  return -1;
}

static void engine_delete_classes(JNIEnv *env, EngineApi *api) {
  for (int i = 0; i < ENGINE_CLASS_COUNT; i++) {
    if (api->classes[i] != NULL) (*env)->DeleteGlobalRef(env, api->classes[i]);
  }
}

static void engine_api_release(EngineApi *api) {
  if (atomic_fetch_sub_explicit(&api->references, 1, memory_order_acq_rel) != 1) return;
  JavaScope scope;
  if (scope_enter(api->context, &scope, NULL) == 0) {
    engine_delete_classes(scope.env, api);
    scope_leave(&scope);
  }
  context_release(api->context);
  free(api);
}

static jclass engine_class(JNIEnv *env, EngineApi *api, const char *name,
    BridgeBuffer *error) {
  jclass local = (*env)->FindClass(env, name);
  if (engine_exception(env, api, error) != 0 || local == NULL) return NULL;
  jclass global = (*env)->NewGlobalRef(env, local);
  (*env)->DeleteLocalRef(env, local);
  if (engine_exception(env, api, error) != 0) return NULL;
  return global;
}

static int engine_method(JNIEnv *env, EngineApi *api, EngineKind kind,
    jmethodID *out, const char *name, const char *signature, BridgeBuffer *error) {
  *out = (*env)->GetMethodID(env, api->classes[kind], name, signature);
  if (engine_exception(env, api, error) != 0 || *out == NULL) return -1;
  return 0;
}

static EngineHandle *engine_handle(JNIEnv *env, EngineApi *api, jobject object,
    EngineKind kind, BridgeBuffer *error) {
  EngineHandle *handle = calloc(1, sizeof(*handle));
  if (handle != NULL) {
    handle->object = (*env)->NewGlobalRef(env, object);
    (void) engine_exception(env, api, error);
  }
  if (handle == NULL || handle->object == NULL) {
    engine_error(error, 13, "cannot retain Java engine handle");
    (*env)->CallVoidMethod(env, object, api->close[kind]);
    (void) engine_exception(env, api, NULL);
    free(handle);
    return NULL;
  }
  handle->api = api;
  handle->kind = kind;
  atomic_fetch_add_explicit(&api->references, 1, memory_order_relaxed);
  return handle;
}

static EngineHandle *engine_factory(JNIEnv *env, jobject factory, BridgeBuffer *error) {
  EngineApi *api = calloc(1, sizeof(*api));
  JavaContext *context = calloc(1, sizeof(*context));
  if (api == NULL || context == NULL) {
    free(api);
    free(context);
    engine_error(error, 13, "cannot allocate Java engine context");
    return NULL;
  }
  api->context = context;
  atomic_init(&api->references, 1);
  atomic_init(&context->references, 1);
  if ((*env)->GetJavaVM(env, &context->vm) != JNI_OK) goto failed;
  context->thread_class = engine_class(env, api, "java/lang/Thread", error);
  if (context->thread_class == NULL) goto failed;
  context->current_thread = (*env)->GetStaticMethodID(env, context->thread_class,
      "currentThread", "()Ljava/lang/Thread;");
  if (engine_exception(env, api, error) != 0 || context->current_thread == NULL) goto failed;
  context->get_loader = (*env)->GetMethodID(env, context->thread_class,
      "getContextClassLoader", "()Ljava/lang/ClassLoader;");
  if (engine_exception(env, api, error) != 0 || context->get_loader == NULL) goto failed;
  context->set_loader = (*env)->GetMethodID(env, context->thread_class,
      "setContextClassLoader", "(Ljava/lang/ClassLoader;)V");
  if (engine_exception(env, api, error) != 0 || context->set_loader == NULL) goto failed;
  jobject current = (*env)->CallStaticObjectMethod(env, context->thread_class,
      context->current_thread);
  if (engine_exception(env, api, error) != 0 || current == NULL) goto failed;
  jobject loader = (*env)->CallObjectMethod(env, current, context->get_loader);
  (*env)->DeleteLocalRef(env, current);
  if (engine_exception(env, api, error) != 0) goto failed;
  if (loader != NULL) {
    context->loader = (*env)->NewGlobalRef(env, loader);
    (*env)->DeleteLocalRef(env, loader);
    if (engine_exception(env, api, error) != 0 || context->loader == NULL) goto failed;
  }
  jclass factory_class = (*env)->GetObjectClass(env, factory);
  if (engine_exception(env, api, error) != 0 || factory_class == NULL) goto failed;
  api->classes[ENGINE_FACTORY] = (*env)->NewGlobalRef(env, factory_class);
  (*env)->DeleteLocalRef(env, factory_class);
  if (engine_exception(env, api, error) != 0 || api->classes[ENGINE_FACTORY] == NULL) goto failed;
  api->classes[ENGINE_TASK] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Task", error);
  if (api->classes[ENGINE_TASK] == NULL) goto failed;
  api->classes[ENGINE_SESSION] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Session", error);
  if (api->classes[ENGINE_SESSION] == NULL) goto failed;
  api->classes[ENGINE_QUERY] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Query", error);
  if (api->classes[ENGINE_QUERY] == NULL) goto failed;
  api->classes[ENGINE_BATCH] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/NativeArrowBatch", error);
  if (api->classes[ENGINE_BATCH] == NULL) goto failed;
  api->classes[ENGINE_NODE] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/NativeArrowBatch$Node", error);
  if (api->classes[ENGINE_NODE] == NULL) goto failed;
  api->classes[ENGINE_EXCEPTION] = engine_class(env, api,
      "org/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$EngineException", error);
  if (api->classes[ENGINE_EXCEPTION] == NULL) goto failed;
  for (int i = ENGINE_FACTORY; i < ENGINE_EXCEPTION; i++) {
    if (engine_method(env, api, (EngineKind) i, &api->close[i], "close", "()V", error) != 0)
      goto failed;
  }
  if (engine_method(env, api, ENGINE_EXCEPTION, &api->exception_code, "code", "()I", error) != 0
      || engine_method(env, api, ENGINE_FACTORY, &api->new_task, "newTask",
          "(Ljava/lang/String;)Lorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Task;",
          error) != 0
      || engine_method(env, api, ENGINE_FACTORY, &api->open_session, "openSession",
          "(Ljava/lang/String;Ljava/lang/String;Lorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Task;)Lorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Session;",
          error) != 0
      || engine_method(env, api, ENGINE_SESSION, &api->prepare, "prepare",
          "([BLorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Task;)Lorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Query;",
          error) != 0
      || engine_method(env, api, ENGINE_SESSION, &api->config, "config",
          "(I[BLorg/apache/spark/sql/connect/nativebridge/engine/SparkJvmEngine$Task;)[B",
          error) != 0
      || engine_method(env, api, ENGINE_TASK, &api->cancel, "cancel", "()V", error) != 0
      || engine_method(env, api, ENGINE_QUERY, &api->schema, "schemaIpc", "()[B", error) != 0
      || engine_method(env, api, ENGINE_QUERY, &api->next, "nextBatch", "()[B", error) != 0
      || engine_method(env, api, ENGINE_QUERY, &api->next_arrow, "nextVectorBatch",
          "()Lorg/apache/spark/sql/connect/nativebridge/engine/NativeArrowBatch;", error) != 0
      || engine_method(env, api, ENGINE_BATCH, &api->descriptor, "descriptor",
          "()Lorg/apache/spark/sql/connect/nativebridge/engine/NativeArrowBatch$Node;", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_length, "length", "()J", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_null_count, "nullCount", "()J", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_offset, "offset", "()J", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_buffers, "buffers", "()[J", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_buffer_sizes, "bufferSizes", "()[J", error) != 0
      || engine_method(env, api, ENGINE_NODE, &api->node_children, "children",
          "()[Lorg/apache/spark/sql/connect/nativebridge/engine/NativeArrowBatch$Node;", error) != 0
      || engine_method(env, api, ENGINE_QUERY, &api->local, "isLocal", "()Z", error) != 0
      || engine_method(env, api, ENGINE_QUERY, &api->streaming, "isStreaming", "()Z", error) != 0)
    goto failed;
  EngineHandle *handle = engine_handle(env, api, factory, ENGINE_FACTORY, error);
  engine_api_release(api);
  return handle;
failed:
  (void) engine_exception(env, api, error);
  engine_normalize_error(error, "cannot initialize Java engine ABI");
  engine_delete_classes(env, api);
  if (context->loader != NULL) (*env)->DeleteGlobalRef(env, context->loader);
  if (context->thread_class != NULL) (*env)->DeleteGlobalRef(env, context->thread_class);
  free(context);
  free(api);
  return NULL;
}

static int engine_check(EngineHandle *handle, EngineKind kind, BridgeBuffer *error) {
  if (handle != NULL && handle->kind == kind) return 0;
  engine_error(error, 13, "invalid Java engine handle");
  return -1;
}

static int engine_task_check(EngineHandle *handle, EngineHandle *task, BridgeBuffer *error) {
  if (engine_check(task, ENGINE_TASK, error) != 0) return -1;
  if (handle->api == task->api) return 0;
  engine_error(error, 13, "Java engine task belongs to another engine");
  return -1;
}

static int engine_scope(EngineHandle *handle, JavaScope *scope, BridgeBuffer *error) {
  if (scope_enter(handle->api->context, scope, error) == 0) return 0;
  engine_normalize_error(error, "cannot enter Java engine context");
  return -1;
}

void *eng_task_new(void *raw_engine, BridgeBuffer *error) {
  clear_buffer(error);
  EngineHandle *engine = raw_engine;
  if (engine_check(engine, ENGINE_FACTORY, error) != 0) return NULL;
  static atomic_uint_fast64_t next_task = ATOMIC_VAR_INIT(1);
  uint_fast64_t id = atomic_load_explicit(&next_task, memory_order_relaxed);
  do {
    if (id == UINT_FAST64_MAX) {
      engine_error(error, 8, "Java engine task identifiers are exhausted");
      return NULL;
    }
  } while (!atomic_compare_exchange_weak_explicit(&next_task, &id, id + 1,
      memory_order_relaxed, memory_order_relaxed));
  char name[64];
  int len = snprintf(name, sizeof(name), "spark-engine-%" PRIuFAST64, id);
  JavaScope scope;
  if (engine_scope(engine, &scope, error) != 0) return NULL;
  JNIEnv *env = scope.env;
  jstring description = utf8_string(env, (const uint8_t *) name, (size_t) len, error);
  jobject object = description == NULL ? NULL : (*env)->CallObjectMethod(env,
      engine->object, engine->api->new_task, description);
  EngineHandle *task = NULL;
  if (engine_exception(env, engine->api, error) == 0 && object != NULL)
    task = engine_handle(env, engine->api, object, ENGINE_TASK, error);
  if (object != NULL) (*env)->DeleteLocalRef(env, object);
  if (description != NULL) (*env)->DeleteLocalRef(env, description);
  scope_leave(&scope);
  if (task == NULL) engine_normalize_error(error, "Java engine did not create a task");
  return task;
}

void *eng_open_session(void *raw_engine, const uint8_t *user, size_t user_len,
    const uint8_t *id, size_t id_len, void *raw_task, BridgeBuffer *error) {
  clear_buffer(error);
  EngineHandle *engine = raw_engine;
  EngineHandle *task = raw_task;
  if (engine_check(engine, ENGINE_FACTORY, error) != 0
      || engine_task_check(engine, task, error) != 0) return NULL;
  JavaScope scope;
  if (engine_scope(engine, &scope, error) != 0) return NULL;
  JNIEnv *env = scope.env;
  jstring user_string = utf8_string(env, user, user_len, error);
  jstring id_string = user_string == NULL ? NULL : utf8_string(env, id, id_len, error);
  jobject object = id_string == NULL ? NULL : (*env)->CallObjectMethod(env,
      engine->object, engine->api->open_session, user_string, id_string, task->object);
  EngineHandle *session = NULL;
  if (engine_exception(env, engine->api, error) == 0 && object != NULL)
    session = engine_handle(env, engine->api, object, ENGINE_SESSION, error);
  if (object != NULL) (*env)->DeleteLocalRef(env, object);
  if (id_string != NULL) (*env)->DeleteLocalRef(env, id_string);
  if (user_string != NULL) (*env)->DeleteLocalRef(env, user_string);
  scope_leave(&scope);
  if (session == NULL) engine_normalize_error(error, "Java engine did not open a session");
  return session;
}

void *eng_prepare(void *raw_session, const uint8_t *relation, size_t len,
    void *raw_task, BridgeBuffer *error) {
  clear_buffer(error);
  EngineHandle *session = raw_session;
  EngineHandle *task = raw_task;
  if (engine_check(session, ENGINE_SESSION, error) != 0
      || engine_task_check(session, task, error) != 0) return NULL;
  JavaScope scope;
  if (engine_scope(session, &scope, error) != 0) return NULL;
  JNIEnv *env = scope.env;
  jbyteArray input = byte_array(env, relation, len, error);
  jobject object = input == NULL ? NULL : (*env)->CallObjectMethod(env,
      session->object, session->api->prepare, input, task->object);
  EngineHandle *query = NULL;
  if (engine_exception(env, session->api, error) == 0 && object != NULL)
    query = engine_handle(env, session->api, object, ENGINE_QUERY, error);
  if (object != NULL) (*env)->DeleteLocalRef(env, object);
  if (input != NULL) (*env)->DeleteLocalRef(env, input);
  scope_leave(&scope);
  if (query == NULL) engine_normalize_error(error, "Java engine did not prepare a query");
  return query;
}

static int engine_query_bytes(EngineHandle *query, int next, BridgeBuffer *out,
    BridgeBuffer *error) {
  clear_buffer(out);
  clear_buffer(error);
  if (engine_check(query, ENGINE_QUERY, error) != 0) return -1;
  if (out == NULL) {
    engine_error(error, 13, "missing Java engine result buffer");
    return -1;
  }
  JavaScope scope;
  if (engine_scope(query, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jbyteArray bytes = (*env)->CallObjectMethod(env, query->object,
      next ? query->api->next : query->api->schema);
  int result = engine_exception(env, query->api, error);
  if (result == 0 && bytes != NULL) {
    result = array_buffer(env, bytes, out, error) == 0 ? (next ? 1 : 0) : -1;
  } else if (result == 0 && !next) {
    engine_error(error, 13, "Java engine returned no schema");
    result = -1;
  }
  if (bytes != NULL) (*env)->DeleteLocalRef(env, bytes);
  scope_leave(&scope);
  if (result < 0) engine_normalize_error(error, "cannot read Java engine query");
  return result;
}

int eng_schema(void *query, BridgeBuffer *out, BridgeBuffer *error) {
  return engine_query_bytes(query, 0, out, error);
}

int eng_next(void *query, BridgeBuffer *out, BridgeBuffer *error) {
  return engine_query_bytes(query, 1, out, error);
}

/* Java owns every off-heap value buffer. Native metadata describes its layout and
 * holds the batch alive until the last imported Rust buffer releases its owner. */
typedef struct {
  atomic_uint references;
  EngineHandle *batch;
  void *lease;
  void (*defer_release)(void *lease, void *owner);
} ArrowExportOwner;

typedef struct {
  ArrowExportOwner *owner;
  int64_t *capacities;
} ArrowNodePrivate;

void eng_arrow_owner_free(void *raw_owner) {
  ArrowExportOwner *owner = raw_owner;
  if (owner == NULL) return;
  eng_free(owner->batch);
  free(owner);
}

static void arrow_owner_unref(ArrowExportOwner *owner) {
  if (atomic_fetch_sub_explicit(&owner->references, 1, memory_order_acq_rel) != 1) return;
  if (owner->defer_release != NULL) owner->defer_release(owner->lease, owner);
  else eng_arrow_owner_free(owner);
}

static void arrow_node_release(struct ArrowArray *array) {
  if (array == NULL || array->release == NULL) return;
  array->release = NULL;
  ArrowNodePrivate *private = array->private_data;
  /* Child structs may have been moved by a C Data consumer. In that case their
   * release pointer is NULL; the moved child's private owner remains retained. */
  if (array->children != NULL) {
    for (int64_t i = 0; i < array->n_children; i++) {
      if (array->children[i] != NULL) {
        if (array->children[i]->release != NULL)
          array->children[i]->release(array->children[i]);
        free(array->children[i]);
      }
    }
  }
  if (array->dictionary != NULL) {
    if (array->dictionary->release != NULL) array->dictionary->release(array->dictionary);
    free(array->dictionary);
  }
  free((void *) array->buffers);
  free(array->children);
  array->buffers = NULL;
  array->children = NULL;
  array->dictionary = NULL;
  array->private_data = NULL;
  if (private != NULL) {
    ArrowExportOwner *owner = private->owner;
    free(private->capacities);
    free(private);
    arrow_owner_unref(owner);
  }
}

int64_t eng_arrow_buffer_size(const struct ArrowArray *array, int64_t index) {
  if (array == NULL || array->release != arrow_node_release || array->private_data == NULL
      || index < 0 || index >= array->n_buffers) return -1;
  const ArrowNodePrivate *private = array->private_data;
  return private->capacities[index];
}

static int arrow_read_long(JNIEnv *env, EngineApi *api, jobject node,
    jmethodID method, int64_t *out, BridgeBuffer *error) {
  *out = (int64_t) (*env)->CallLongMethod(env, node, method);
  return engine_exception(env, api, error);
}

static int arrow_build_node(JNIEnv *env, EngineApi *api, jobject node,
    ArrowExportOwner *owner, struct ArrowArray *out, unsigned depth,
    size_t *remaining_nodes, BridgeBuffer *error) {
  if (node == NULL || depth > 128 || *remaining_nodes == 0) {
    engine_error(error, 13, "invalid or excessive Java Arrow descriptor tree");
    return -1;
  }
  (*remaining_nodes)--;
  /* Nested descriptors retain a handful of local references at each level. */
  jint capacity = (*env)->EnsureLocalCapacity(env, 8);
  if (engine_exception(env, api, error) != 0 || capacity != JNI_OK) {
    engine_normalize_error(error, "cannot reserve Java Arrow descriptor references");
    return -1;
  }
  if (!(*env)->IsInstanceOf(env, node, api->classes[ENGINE_NODE])) {
    engine_error(error, 13, "invalid Java Arrow node type");
    return -1;
  }
  ArrowNodePrivate *private = calloc(1, sizeof(*private));
  if (private == NULL) {
    engine_error(error, 8, "cannot allocate Arrow descriptor");
    return -1;
  }
  private->owner = owner;
  atomic_fetch_add_explicit(&owner->references, 1, memory_order_relaxed);
  out->private_data = private;
  out->release = arrow_node_release;
  jlongArray buffers = NULL;
  jlongArray capacities = NULL;
  jobjectArray children = NULL;
  int result = -1;
  if (arrow_read_long(env, api, node, api->node_length, &out->length, error) != 0
      || arrow_read_long(env, api, node, api->node_null_count, &out->null_count, error) != 0
      || arrow_read_long(env, api, node, api->node_offset, &out->offset, error) != 0) goto done;
  if (out->length < 0 || out->null_count < 0 || out->null_count > out->length
      || out->offset != 0) {
    engine_error(error, 13, "invalid Java Arrow node length, null count, or offset");
    goto done;
  }
  buffers = (*env)->CallObjectMethod(env, node, api->node_buffers);
  if (engine_exception(env, api, error) != 0 || buffers == NULL) goto done;
  capacities = (*env)->CallObjectMethod(env, node, api->node_buffer_sizes);
  if (engine_exception(env, api, error) != 0 || capacities == NULL) goto done;
  children = (*env)->CallObjectMethod(env, node, api->node_children);
  if (engine_exception(env, api, error) != 0 || children == NULL) goto done;
  jsize n_buffers = (*env)->GetArrayLength(env, buffers);
  if (engine_exception(env, api, error) != 0) goto done;
  jsize n_capacities = (*env)->GetArrayLength(env, capacities);
  if (engine_exception(env, api, error) != 0) goto done;
  jsize n_children = (*env)->GetArrayLength(env, children);
  if (engine_exception(env, api, error) != 0) goto done;
  if (n_buffers != n_capacities || n_buffers > 1024 || n_children > 10000) {
    engine_error(error, 13, "invalid Java Arrow descriptor dimensions");
    goto done;
  }
  out->n_buffers = n_buffers;
  out->n_children = n_children;
  if (n_buffers != 0) {
    out->buffers = calloc((size_t) n_buffers, sizeof(*out->buffers));
    private->capacities = calloc((size_t) n_buffers, sizeof(*private->capacities));
    if (out->buffers == NULL || private->capacities == NULL) {
      engine_error(error, 8, "cannot allocate Arrow buffer descriptors");
      goto done;
    }
    for (jsize i = 0; i < n_buffers; i++) {
      jlong address;
      jlong capacity;
      (*env)->GetLongArrayRegion(env, buffers, i, 1, &address);
      if (engine_exception(env, api, error) != 0) goto done;
      (*env)->GetLongArrayRegion(env, capacities, i, 1, &capacity);
      if (engine_exception(env, api, error) != 0) goto done;
      if (capacity < 0 || (uint64_t) capacity > SIZE_MAX
          || ((uint64_t) address > UINTPTR_MAX)
          || (capacity != 0 && address == 0)) {
        engine_error(error, 13, "invalid Java Arrow buffer address or capacity");
        goto done;
      }
      out->buffers[i] = (const void *) (uintptr_t) (uint64_t) address;
      private->capacities[i] = capacity;
    }
  }
  if (n_children != 0) {
    out->children = calloc((size_t) n_children, sizeof(*out->children));
    if (out->children == NULL) {
      engine_error(error, 8, "cannot allocate Arrow child descriptors");
      goto done;
    }
    for (jsize i = 0; i < n_children; i++) {
      jobject child = (*env)->GetObjectArrayElement(env, children, i);
      if (engine_exception(env, api, error) != 0) {
        if (child != NULL) (*env)->DeleteLocalRef(env, child);
        goto done;
      }
      out->children[i] = calloc(1, sizeof(*out->children[i]));
      if (out->children[i] == NULL) {
        if (child != NULL) (*env)->DeleteLocalRef(env, child);
        engine_error(error, 8, "cannot allocate Arrow child descriptor");
        goto done;
      }
      int child_result = arrow_build_node(env, api, child, owner, out->children[i],
          depth + 1, remaining_nodes, error);
      if (child != NULL) (*env)->DeleteLocalRef(env, child);
      if (child_result != 0) goto done;
    }
  }
  result = 0;
done:
  if (children != NULL) (*env)->DeleteLocalRef(env, children);
  if (capacities != NULL) (*env)->DeleteLocalRef(env, capacities);
  if (buffers != NULL) (*env)->DeleteLocalRef(env, buffers);
  if (result != 0) {
    arrow_node_release(out);
    engine_normalize_error(error, "cannot export Java Arrow descriptor");
  }
  return result;
}

int eng_next_arrow(void *raw_query, struct ArrowArray *out, void *lease,
    void (*defer_release)(void *lease, void *owner), BridgeBuffer *error) {
  clear_buffer(error);
  EngineHandle *query = raw_query;
  if (engine_check(query, ENGINE_QUERY, error) != 0) return -1;
  if (out == NULL || out->release != NULL || lease == NULL || defer_release == NULL) {
    engine_error(error, 13, "invalid Java Arrow export destination or lease");
    return -1;
  }
  memset(out, 0, sizeof(*out));
  JavaScope scope;
  if (engine_scope(query, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jobject batch = (*env)->CallObjectMethod(env, query->object, query->api->next_arrow);
  int result = engine_exception(env, query->api, error);
  if (result != 0 || batch == NULL) {
    if (batch != NULL) {
      (*env)->CallVoidMethod(env, batch, query->api->close[ENGINE_BATCH]);
      (void) engine_exception(env, query->api, NULL);
      (*env)->DeleteLocalRef(env, batch);
    }
    scope_leave(&scope);
    return result;
  }
  EngineHandle *batch_handle = engine_handle(env, query->api, batch, ENGINE_BATCH, error);
  ArrowExportOwner *owner = NULL;
  if (batch_handle != NULL) owner = calloc(1, sizeof(*owner));
  if (batch_handle == NULL || owner == NULL) {
    if (batch_handle != NULL) eng_free(batch_handle);
    engine_error(error, 8, "cannot allocate Java Arrow export owner");
    (*env)->DeleteLocalRef(env, batch);
    scope_leave(&scope);
    return -1;
  }
  atomic_init(&owner->references, 1); /* Temporary build owner. */
  owner->batch = batch_handle;
  jobject node = (*env)->CallObjectMethod(env, batch, query->api->descriptor);
  result = engine_exception(env, query->api, error);
  size_t remaining_nodes = 100000;
  if (result == 0) result = arrow_build_node(env, query->api, node, owner, out, 0,
      &remaining_nodes, error);
  if (node != NULL) (*env)->DeleteLocalRef(env, node);
  (*env)->DeleteLocalRef(env, batch);
  scope_leave(&scope);
  if (result == 0) {
    owner->lease = lease;
    owner->defer_release = defer_release;
  }
  arrow_owner_unref(owner);
  if (result != 0) {
    engine_normalize_error(error, "cannot export Java Arrow batch");
    return -1;
  }
  return 1;
}

int eng_flags(void *raw_query, int32_t *flags, BridgeBuffer *error) {
  clear_buffer(error);
  if (flags != NULL) *flags = 0;
  EngineHandle *query = raw_query;
  if (engine_check(query, ENGINE_QUERY, error) != 0) return -1;
  if (flags == NULL) {
    engine_error(error, 13, "missing Java engine flag buffer");
    return -1;
  }
  JavaScope scope;
  if (engine_scope(query, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jboolean local = (*env)->CallBooleanMethod(env, query->object, query->api->local);
  int result = engine_exception(env, query->api, error);
  if (result == 0) {
    jboolean streaming = (*env)->CallBooleanMethod(env, query->object, query->api->streaming);
    result = engine_exception(env, query->api, error);
    if (result == 0) *flags = (local ? 1 : 0) | (streaming ? 2 : 0);
  }
  scope_leave(&scope);
  return result;
}

int eng_config(void *raw_session, int32_t opcode, const uint8_t *fields, size_t len,
    void *raw_task, BridgeBuffer *out, BridgeBuffer *error) {
  clear_buffer(out);
  clear_buffer(error);
  EngineHandle *session = raw_session;
  EngineHandle *task = raw_task;
  if (engine_check(session, ENGINE_SESSION, error) != 0
      || engine_task_check(session, task, error) != 0) return -1;
  if (out == NULL) {
    engine_error(error, 13, "missing Java engine config buffer");
    return -1;
  }
  JavaScope scope;
  if (engine_scope(session, &scope, error) != 0) return -1;
  JNIEnv *env = scope.env;
  jbyteArray input = byte_array(env, fields, len, error);
  jbyteArray bytes = input == NULL ? NULL : (*env)->CallObjectMethod(env,
      session->object, session->api->config, (jint) opcode, input, task->object);
  int result = engine_exception(env, session->api, error);
  if (result == 0 && bytes != NULL) result = array_buffer(env, bytes, out, error);
  else if (result == 0) {
    engine_normalize_error(error, "Java engine returned no config result");
    result = -1;
  }
  if (bytes != NULL) (*env)->DeleteLocalRef(env, bytes);
  if (input != NULL) (*env)->DeleteLocalRef(env, input);
  scope_leave(&scope);
  if (result < 0) engine_normalize_error(error, "cannot configure Java engine session");
  return result;
}

void eng_cancel(void *raw_task) {
  EngineHandle *task = raw_task;
  if (engine_check(task, ENGINE_TASK, NULL) != 0) return;
  JavaScope scope;
  if (engine_scope(task, &scope, NULL) != 0) return;
  (*scope.env)->CallVoidMethod(scope.env, task->object, task->api->cancel);
  (void) engine_exception(scope.env, task->api, NULL);
  scope_leave(&scope);
}

void eng_free(void *raw_handle) {
  EngineHandle *handle = raw_handle;
  if (handle == NULL) return;
  EngineApi *api = handle->api;
  JavaScope scope;
  BridgeBuffer error = {0};
  if (engine_scope(handle, &scope, &error) == 0) {
    (*scope.env)->CallVoidMethod(scope.env, handle->object, api->close[handle->kind]);
    (void) engine_exception(scope.env, api, &error);
    (*scope.env)->DeleteGlobalRef(scope.env, handle->object);
    scope_leave(&scope);
  }
  if (error.data != NULL) {
    fputs("Java engine handle cleanup failed: ", stderr);
    (void) fwrite(error.data, 1, error.len, stderr);
    fputc('\n', stderr);
    bridge_buffer_free(&error);
  }
  free(handle);
  engine_api_release(api);
}

int64_t eng_native_start(void *raw_env, void *clazz, void *raw_address, void *raw_factory) {
  (void) clazz;
  JNIEnv *env = raw_env;
  BridgeBuffer error = {0};
  BridgeBuffer address = {0};
  if (raw_address == NULL || raw_factory == NULL) {
    engine_error(&error, 3, "native address and engine factory must be non-null");
    throw_error(env, &error);
    return 0;
  }
  if (array_buffer(env, raw_address, &address, &error) != 0) {
    engine_normalize_error(&error, "cannot read native engine address");
    throw_error(env, &error);
    return 0;
  }
  EngineHandle *factory = engine_factory(env, raw_factory, &error);
  if (factory == NULL) {
    bridge_buffer_free(&address);
    throw_error(env, &error);
    return 0;
  }
  /* ferrum_engine_start owns factory on both success and failure. */
  int64_t id = ferrum_engine_start(address.data, address.len, factory, &error);
  bridge_buffer_free(&address);
  if (id == 0) throw_error(env, &error);
  else bridge_buffer_free(&error);
  return id;
}

void *eng_native_address(void *env, void *clazz, int64_t id) {
  return bridge_native_address(env, clazz, id);
}

void eng_native_stop(void *env, void *clazz, int64_t id) {
  bridge_native_stop(env, clazz, id);
}
