// The merchants' gRPC API, generated from proto/ (shared with the TypeScript SDK).
import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("com.google.protobuf") version "0.10.0"
}

val grpcVersion = "1.85.1"
val protobufVersion = "4.36.2"

dependencies {
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("io.grpc:grpc-stub:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
}

sourceSets {
    main {
        proto { srcDir(rootProject.file("proto")) }
    }
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
    }
    generateProtoTasks {
        all().forEach { it.plugins { id("grpc") } }
    }
}
