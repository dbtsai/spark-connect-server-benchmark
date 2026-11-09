package org.apache.spark.sql.connect.nativebridge.benchmark;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.spark.connect.proto.SparkConnectServiceGrpc;
import org.sparkproject.connect.grpc.MethodDescriptor;
import org.sparkproject.connect.grpc.ServerMethodDefinition;
import org.sparkproject.connect.grpc.ServerServiceDefinition;
import org.sparkproject.connect.protobuf.Descriptors;
import org.sparkproject.connect.protobuf.DynamicMessage;

/** Runtime checks against shared wire vectors independently encoded by Rust prost tests. */
public final class GetStatusCompatTest {
  private GetStatusCompatTest() {}

  public static void main(String[] arguments) throws Exception {
    require(arguments.length == 1, "pass benchmarks/protocol/testdata/get-status");
    Path vectors = Path.of(arguments[0]);
    ServerServiceDefinition original = new SparkConnectServiceGrpc.SparkConnectServiceImplBase() {}
        .bindService();
    Map<String, Integer> states = new LinkedHashMap<>();
    states.put("b", 2);
    states.put("missing", 1);
    ServerServiceDefinition bound = GetStatusCompat.bind(original, (request, observer) -> {
      observer.onNext(GetStatusCompat.response(new GetStatusCompat.Input(request).session, states));
      observer.onCompleted();
    });
    MethodDescriptor<DynamicMessage, DynamicMessage> method = statusMethod(bound);
    require(method.getType() == MethodDescriptor.MethodType.UNARY, "GetStatus is not unary");
    long originalOtherRoutes = original.getMethods().stream()
        .filter(route -> !route.getMethodDescriptor().getFullMethodName().equals(GetStatusCompat.FULL_METHOD))
        .count();
    require(bound.getMethods().size() == originalOtherRoutes + 1, "route binding changed other routes");
    for (ServerMethodDefinition<?, ?> route : original.getMethods()) {
      if (!route.getMethodDescriptor().getFullMethodName().equals(GetStatusCompat.FULL_METHOD)) {
        require(bound.getMethods().contains(route), "route binding replaced an unrelated handler");
      }
    }

    byte[] requestBytes = bytes(vectors, "request.hex");
    DynamicMessage request = method.getRequestMarshaller().parse(new ByteArrayInputStream(requestBytes));
    GetStatusCompat.Input input = new GetStatusCompat.Input(request);
    require(input.session.equals("s") && input.hasUser && input.user.getUserId().equals("u"),
        "request identity changed");
    require(input.hasObserved && input.observed.isEmpty() && input.hasOperationStatus,
        "present empty optional values lost presence");
    require(input.operationIds.equals(Arrays.asList("b", "a", "b")), "selector wire order changed");
    Descriptors.Descriptor descriptor = request.getDescriptorForType();
    for (int number : new int[] {3, 4, 5}) {
      Descriptors.FieldDescriptor field = descriptor.findFieldByNumber(number);
      require(field.toProto().getProto3Optional() && request.hasField(field),
          "optional descriptor/presence changed for field " + number);
    }
    require(request.getField(descriptor.findFieldByNumber(3)).equals(""), "empty client_type changed");
    require(request.getRepeatedFieldCount(descriptor.findFieldByNumber(999)) == 1,
        "request extensions were not decoded");
    Descriptors.Descriptor selector = descriptor.findFieldByNumber(5).getMessageType();
    require(selector.findFieldByNumber(1).isRepeated()
        && selector.findFieldByNumber(999).isRepeated(), "selector descriptor changed");
    require(Arrays.equals(requestBytes, method.getRequestMarshaller().stream(request).readAllBytes()),
        "request DynamicMessage marshaller did not round trip prost bytes");

    GetStatusCompat.Input absent = new GetStatusCompat.Input(method.getRequestMarshaller().parse(
        new ByteArrayInputStream(bytes(vectors, "absent-request.hex"))));
    require(!absent.hasObserved && !absent.hasOperationStatus && absent.operationIds.isEmpty(),
        "absent selector became present");
    DynamicMessage emptyMessage = method.getRequestMarshaller().parse(
        new ByteArrayInputStream(bytes(vectors, "empty-request.hex")));
    GetStatusCompat.Input empty = new GetStatusCompat.Input(emptyMessage);
    require(empty.hasObserved && empty.observed.isEmpty() && empty.hasOperationStatus
        && empty.operationIds.isEmpty(), "present empty selector lost presence");
    require(emptyMessage.hasField(descriptor.findFieldByNumber(3)), "empty client_type lost presence");

    DynamicMessage response = GetStatusCompat.response("s", states);
    byte[] responseBytes = bytes(vectors, "response.hex");
    require(Arrays.equals(responseBytes, method.getResponseMarshaller().stream(response).readAllBytes()),
        "response DynamicMessage marshaller differs from prost bytes");
    DynamicMessage parsedResponse = method.getResponseMarshaller().parse(new ByteArrayInputStream(responseBytes));
    require(parsedResponse.equals(response), "response DynamicMessage round trip changed fields");
    Descriptors.Descriptor operation = response.getDescriptorForType().findNestedTypeByName("OperationStatus");
    Descriptors.EnumDescriptor state = operation.findFieldByNumber(2).getEnumType();
    String[] names = {"UNSPECIFIED", "UNKNOWN", "RUNNING", "TERMINATING", "SUCCEEDED", "FAILED", "CANCELLED"};
    for (int number = 0; number < names.length; number++) {
      require(state.findValueByNumber(number).getName().equals("OPERATION_STATE_" + names[number]),
          "operation state enum changed");
    }
    require(operation.findFieldByNumber(999).isRepeated(), "operation extensions descriptor changed");
    require(response.getDescriptorForType().findFieldByNumber(999).isRepeated(),
        "response extensions descriptor changed");
    System.out.println("GET_STATUS_COMPAT_OK " + GetStatusCompat.descriptorSource()
        + "; prost wire vectors; optional presence; request/response marshalling; route preservation");
  }

  @SuppressWarnings("unchecked")
  private static MethodDescriptor<DynamicMessage, DynamicMessage> statusMethod(ServerServiceDefinition service) {
    for (ServerMethodDefinition<?, ?> method : service.getMethods()) {
      if (method.getMethodDescriptor().getFullMethodName().equals(GetStatusCompat.FULL_METHOD)) {
        return (MethodDescriptor<DynamicMessage, DynamicMessage>) method.getMethodDescriptor();
      }
    }
    throw new AssertionError("GetStatus route missing");
  }

  private static byte[] bytes(Path directory, String name) throws Exception {
    return HexFormat.of().parseHex(Files.readString(directory.resolve(name)).trim());
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
