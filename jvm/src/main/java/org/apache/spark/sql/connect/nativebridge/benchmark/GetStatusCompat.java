package org.apache.spark.sql.connect.nativebridge.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.connect.proto.Base;
import org.apache.spark.connect.proto.UserContext;
import org.sparkproject.connect.grpc.MethodDescriptor;
import org.sparkproject.connect.grpc.ServerMethodDefinition;
import org.sparkproject.connect.grpc.ServerServiceDefinition;
import org.sparkproject.connect.grpc.protobuf.ProtoUtils;
import org.sparkproject.connect.grpc.stub.ServerCalls;
import org.sparkproject.connect.protobuf.Any;
import org.sparkproject.connect.protobuf.DescriptorProtos;
import org.sparkproject.connect.protobuf.Descriptors;
import org.sparkproject.connect.protobuf.DynamicMessage;
import org.sparkproject.connect.protobuf.Message;

/**
 * GetStatus messages and route for the synthetic fixture when its Spark assembly predates them.
 * The fallback descriptor mirrors GetStatusRequest/GetStatusResponse from the pinned official
 * Rust client's spark/connect/base.proto, including field numbers, presence, extensions and enum:
 * https://github.com/apache/spark-connect-rust/blob/925dc876f63f1f779fe176f62575a0aa96cc6886/
 * crates/spark-connect-proto/proto/spark/connect/base.proto#L1228-L1308
 *
 * DynamicMessage uses Spark's shaded protobuf parser and serializer. This compatibility route
 * belongs only to ProtocolFixtureServer; it does not add support to the real Spark server.
 */
final class GetStatusCompat {
  static final String FULL_METHOD = "spark.connect.SparkConnectService/GetStatus";
  private static final Descriptors.FileDescriptor FILE = descriptor();
  private static final Descriptors.Descriptor REQUEST = FILE.findMessageTypeByName("GetStatusRequest");
  private static final Descriptors.Descriptor RESPONSE = FILE.findMessageTypeByName("GetStatusResponse");
  private static final Descriptors.Descriptor STATUS = RESPONSE.findNestedTypeByName("OperationStatus");
  private static final Descriptors.FieldDescriptor SESSION = REQUEST.findFieldByNumber(1);
  private static final Descriptors.FieldDescriptor USER = REQUEST.findFieldByNumber(2);
  private static final Descriptors.FieldDescriptor OBSERVED = REQUEST.findFieldByNumber(4);
  private static final Descriptors.FieldDescriptor SELECTOR = REQUEST.findFieldByNumber(5);
  private static final Descriptors.FieldDescriptor IDS =
      SELECTOR.getMessageType().findFieldByNumber(1);
  private static final Descriptors.FieldDescriptor RESPONSE_SESSION = RESPONSE.findFieldByNumber(1);
  private static final Descriptors.FieldDescriptor RESPONSE_SERVER = RESPONSE.findFieldByNumber(2);
  private static final Descriptors.FieldDescriptor STATUSES = RESPONSE.findFieldByNumber(3);
  private static final Descriptors.FieldDescriptor OPERATION = STATUS.findFieldByNumber(1);
  private static final Descriptors.FieldDescriptor STATE = STATUS.findFieldByNumber(2);
  private static final MethodDescriptor<DynamicMessage, DynamicMessage> METHOD =
      MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(FULL_METHOD)
          .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(REQUEST)))
          .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(RESPONSE)))
          .build();

  private GetStatusCompat() {}

  static String descriptorSource() {
    return FILE == Base.getDescriptor() ? "assembly-generated; dynamic protobuf marshalling"
        : "official-rust-925dc876f63f1f779fe176f62575a0aa96cc6886-schema; "
            + "fixture compatibility; dynamic protobuf marshalling";
  }

  static ServerServiceDefinition bind(ServerServiceDefinition original,
      ServerCalls.UnaryMethod<DynamicMessage, DynamicMessage> handler) {
    ServerServiceDefinition.Builder service = ServerServiceDefinition.builder(
        original.getServiceDescriptor().getName());
    for (ServerMethodDefinition<?, ?> method : original.getMethods()) {
      // Later assemblies may already generate GetStatus; retain the fixture's implementation.
      if (!method.getMethodDescriptor().getFullMethodName().equals(FULL_METHOD)) service.addMethod(method);
    }
    return service.addMethod(METHOD, ServerCalls.asyncUnaryCall(handler)).build();
  }

  static final class Input {
    final String session;
    final UserContext user;
    final boolean hasUser;
    final String observed;
    final boolean hasObserved;
    final boolean hasOperationStatus;
    final List<String> operationIds;

    Input(DynamicMessage input) {
      session = (String) input.getField(SESSION);
      hasUser = input.hasField(USER);
      user = UserContext.newBuilder().mergeFrom((Message) input.getField(USER)).build();
      observed = (String) input.getField(OBSERVED);
      hasObserved = input.hasField(OBSERVED);
      hasOperationStatus = input.hasField(SELECTOR);
      Message selector = (Message) input.getField(SELECTOR);
      operationIds = new ArrayList<>(selector.getRepeatedFieldCount(IDS));
      for (int index = 0; index < selector.getRepeatedFieldCount(IDS); index++) {
        operationIds.add((String) selector.getRepeatedField(IDS, index));
      }
    }
  }

  static DynamicMessage response(String session, Map<String, Integer> statuses) {
    DynamicMessage.Builder response = DynamicMessage.newBuilder(RESPONSE)
        .setField(RESPONSE_SESSION, session).setField(RESPONSE_SERVER, session);
    statuses.forEach((id, state) -> response.addRepeatedField(STATUSES,
        DynamicMessage.newBuilder(STATUS).setField(OPERATION, id)
            .setField(STATE, STATE.getEnumType().findValueByNumber(state)).build()));
    return response.build();
  }

  private static DescriptorProtos.FieldDescriptorProto.Builder field(String name, int number,
      DescriptorProtos.FieldDescriptorProto.Type type) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
        .setType(type).setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
  }

  private static DescriptorProtos.FieldDescriptorProto extensions() {
    return field("extensions", 999, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
        .setTypeName(".google.protobuf.Any")
        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED).build();
  }

  private static Descriptors.FileDescriptor descriptor() {
    Descriptors.FileDescriptor base = Base.getDescriptor();
    if (base.findMessageTypeByName("GetStatusRequest") != null
        && base.findMessageTypeByName("GetStatusResponse") != null) return base;
    DescriptorProtos.DescriptorProto selector = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("OperationStatusRequest")
        .addField(field("operation_ids", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
        .addField(extensions()).build();
    DescriptorProtos.DescriptorProto request = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("GetStatusRequest").addNestedType(selector)
        .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName("_client_type"))
        .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder()
            .setName("_client_observed_server_side_session_id"))
        .addOneofDecl(DescriptorProtos.OneofDescriptorProto.newBuilder().setName("_operation_status"))
        .addField(field("session_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("user_context", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".spark.connect.UserContext"))
        .addField(field("client_type", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setProto3Optional(true).setOneofIndex(0))
        .addField(field("client_observed_server_side_session_id", 4,
            DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setProto3Optional(true).setOneofIndex(1))
        .addField(field("operation_status", 5, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".spark.connect.GetStatusRequest.OperationStatusRequest")
            .setProto3Optional(true).setOneofIndex(2))
        .addField(extensions()).build();
    DescriptorProtos.EnumDescriptorProto.Builder states = DescriptorProtos.EnumDescriptorProto.newBuilder()
        .setName("OperationState");
    String[] names = {"UNSPECIFIED", "UNKNOWN", "RUNNING", "TERMINATING", "SUCCEEDED", "FAILED", "CANCELLED"};
    for (int index = 0; index < names.length; index++) {
      states.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
          .setName("OPERATION_STATE_" + names[index]).setNumber(index));
    }
    DescriptorProtos.DescriptorProto status = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("OperationStatus").addEnumType(states)
        .addField(field("operation_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("state", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
            .setTypeName(".spark.connect.GetStatusResponse.OperationStatus.OperationState"))
        .addField(extensions()).build();
    DescriptorProtos.DescriptorProto response = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("GetStatusResponse").addNestedType(status)
        .addField(field("session_id", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("server_side_session_id", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
        .addField(field("operation_statuses", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(".spark.connect.GetStatusResponse.OperationStatus")
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
        .addField(extensions()).build();
    DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("spark/connect/benchmark_get_status_compat.proto").setPackage("spark.connect")
        .setSyntax("proto3").addDependency(base.getName()).addDependency(Any.getDescriptor().getFile().getName())
        .addMessageType(request).addMessageType(response).build();
    try {
      return Descriptors.FileDescriptor.buildFrom(file,
          new Descriptors.FileDescriptor[] {base, Any.getDescriptor().getFile()});
    } catch (Descriptors.DescriptorValidationException error) {
      throw new ExceptionInInitializerError(error);
    }
  }
}
