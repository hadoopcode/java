package com.hp.coprocessor;


import com.google.protobuf.*;

public class InsertStudentCoprocessor implements Service {
    public Descriptors.ServiceDescriptor getDescriptorForType() {
        return null;
    }

    public void callMethod(Descriptors.MethodDescriptor method, RpcController controller, Message request, RpcCallback<Message> done) {

    }

    public Message getRequestPrototype(Descriptors.MethodDescriptor method) {
        return null;
    }

    public Message getResponsePrototype(Descriptors.MethodDescriptor method) {
        return null;
    }
}
