/**
 * @file connector_tests.cc
 * @brief Unit tests for connector protocol helpers
 * @author Luis Fernandes (luisrobertantonio.fernandes01@student.csulb.edu)
 * @note Origin: Long Beach Rocketry
 */

#include <gtest/gtest.h>

#include "connector/local_tcp_transport.h"
#include "connector/local_udp_transport.h"
#include "connector/local_zmq_transport.h"
#include "connector/message.h"
#include "connector/ndjson_file.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace {
    std::filesystem::path temp_file_path(const std::string &name) {
        return std::filesystem::temp_directory_path() / name;
    }
}

TEST(ConnectorTests, MessageRoundTripJson) {
    connector::ConnectorMessage message;
    message.schema_version = 1;
    message.message_type = "telemetry_frame";
    message.sequence = 7;
    message.timestamp_ms = 1234567890;
    message.source = "sx1262";
    message.payload = {0x01, 0x02, 0x03, 0x04};
    message.checksum_hex = "AB12";
    message.metadata["frame_type"] = "telemetry";

    const std::string json_text = message.to_json();
    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(json_text);

    EXPECT_EQ(parsed.schema_version, 1);
    EXPECT_EQ(parsed.message_type, "telemetry_frame");
    EXPECT_EQ(parsed.sequence, 7u);
    EXPECT_EQ(parsed.timestamp_ms, 1234567890);
    EXPECT_EQ(parsed.source, std::string("sx1262"));
    EXPECT_EQ(parsed.payload, message.payload);
    EXPECT_TRUE(parsed.checksum_hex.has_value());
    EXPECT_EQ(*parsed.checksum_hex, "AB12");
    EXPECT_EQ(parsed.metadata.at("frame_type"), "telemetry");
}

TEST(ConnectorTests, SourceConversionRoundTrip) {
    EXPECT_EQ(connector::source_to_string(connector::Source::Sx1262), "sx1262");
    EXPECT_EQ(connector::source_to_string(connector::Source::Sx127), "sx127");
    EXPECT_EQ(connector::source_to_string(connector::Source::Simulated), "simulated");

    EXPECT_EQ(connector::source_from_string("sx1262"), connector::Source::Sx1262);
    EXPECT_EQ(connector::source_from_string("sx127"), connector::Source::Sx127);
    EXPECT_EQ(connector::source_from_string("simulated"), connector::Source::Simulated);
}

TEST(ConnectorTests, SourceFromStringRejectsInvalidValue) {
    EXPECT_THROW(static_cast<void>(connector::source_from_string("bad-source")), std::runtime_error);
}

TEST(ConnectorTests, MessageRoundTripWithoutOptionalFields) {
    connector::ConnectorMessage message;
    message.sequence = 12;
    message.timestamp_ms = 333;
    message.source = "sx127";
    message.payload = {0x10};

    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(message.to_json());
    EXPECT_EQ(parsed.sequence, 12u);
    EXPECT_EQ(parsed.timestamp_ms, 333);
    EXPECT_EQ(parsed.source, std::string("sx127"));
    EXPECT_FALSE(parsed.checksum_hex.has_value());
    EXPECT_TRUE(parsed.metadata.empty());
}

TEST(ConnectorTests, MessageToJsonRejectsUnsupportedSchemaVersion) {
    connector::ConnectorMessage message;
    message.schema_version = 0;
    message.payload = {0x01};

    EXPECT_THROW(static_cast<void>(message.to_json()), std::runtime_error);
}

TEST(ConnectorTests, MessageToJsonRejectsUnsupportedMessageType) {
    connector::ConnectorMessage message;
    message.message_type.clear();
    message.payload = {0x01};

    EXPECT_THROW(static_cast<void>(message.to_json()), std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsMissingFields) {
    const std::string missing_sequence =
        R"({"schema_version":1,"message_type":"telemetry_frame","timestamp_ms":1,"source":"sx1262","payload_b64":"AQ=="})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(missing_sequence)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsInvalidBase64) {
    const std::string invalid_payload =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"sx1262","payload_b64":"@@"})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(invalid_payload)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonAllowsCustomSource) {
    const std::string custom_source =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"vehicle.alpha","payload_b64":"AQ=="})";
    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(custom_source);
    EXPECT_EQ(parsed.source, std::string("vehicle.alpha"));
}

TEST(ConnectorTests, MessageFromJsonRejectsSchemaVersionZero) {
    const std::string invalid_schema =
        R"({"schema_version":0,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"simulated","payload_b64":"AQ=="})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(invalid_schema)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsEmptyMessageType) {
    const std::string invalid_type =
        R"({"schema_version":1,"message_type":"","sequence":1,"timestamp_ms":1,"source":"simulated","payload_b64":"AQ=="})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(invalid_type)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsEmptySource) {
    const std::string invalid_source =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"","payload_b64":"AQ=="})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(invalid_source)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonParsesEscapedMetadata) {
    const std::string json_text =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":5,"timestamp_ms":7,"source":"simulated","payload_b64":"AQ==","metadata":{"note":"line\nvalue","quoted":"a\"b"}})";

    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(json_text);
    EXPECT_EQ(parsed.metadata.at("note"), std::string("line\nvalue"));
    EXPECT_EQ(parsed.metadata.at("quoted"), std::string("a\"b"));
}

TEST(ConnectorTests, SourceToStringFallbackForUnknownEnumValue) {
    const auto unknown = static_cast<connector::Source>(999);
    EXPECT_EQ(connector::source_to_string(unknown), "simulated");
}

TEST(ConnectorTests, MessageFromJsonRejectsInvalidEscapes) {
    const std::string invalid_escape =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"simulated","payload_b64":"AQ==","metadata":{"bad":"a\q"}})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(invalid_escape)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsUnsupportedUnicodeEscape) {
    const std::string unicode_escape =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"simulated","payload_b64":"AQ==","metadata":{"bad":"\u0041"}})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(unicode_escape)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsMalformedMetadataObject) {
    const std::string malformed_metadata =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":1,"source":"simulated","payload_b64":"AQ==","metadata":"bad"})";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(malformed_metadata)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonRejectsUnterminatedMetadataObject) {
    const std::string unterminated_metadata =
        "{\"schema_version\":1,\"message_type\":\"telemetry_frame\",\"sequence\":1,\"timestamp_ms\":1,\"source\":\"simulated\",\"payload_b64\":\"AQ==\",\"metadata\":{\"k\":\"v\"";
    EXPECT_THROW(static_cast<void>(connector::ConnectorMessage::from_json(unterminated_metadata)),
                 std::runtime_error);
}

TEST(ConnectorTests, MessageFromJsonAcceptsNegativeTimestamp) {
    const std::string negative_ts =
        R"({"schema_version":1,"message_type":"telemetry_frame","sequence":1,"timestamp_ms":-7,"source":"simulated","payload_b64":"AQ=="})";
    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(negative_ts);
    EXPECT_EQ(parsed.timestamp_ms, -7);
}

TEST(ConnectorTests, MessageRoundTripWithEscapedPayloadMetadataStrings) {
    connector::ConnectorMessage message;
    message.sequence = 88;
    message.timestamp_ms = 777;
    message.source = "simulated";
    message.payload = {0x00, 0x0A, 0x1F};
    message.metadata["path"] = "C:\\tmp\\file";
    message.metadata["quote"] = "\"quoted\"";
    message.metadata["tabs"] = "a\tb";

    const connector::ConnectorMessage parsed =
        connector::ConnectorMessage::from_json(message.to_json());
    EXPECT_EQ(parsed.metadata.at("path"), std::string("C:\\tmp\\file"));
    EXPECT_EQ(parsed.metadata.at("quote"), std::string("\"quoted\""));
    EXPECT_EQ(parsed.metadata.at("tabs"), std::string("a\tb"));
}

TEST(ConnectorTests, MessageToJsonEscapesControlCharacters) {
    connector::ConnectorMessage message;
    message.sequence = 1;
    message.timestamp_ms = 2;
    message.source = "simulated";
    message.payload = {0x01};
    message.metadata["ctrl"] = std::string("a") + '\b' + '\f' + '\n' + '\r' + '\t' + '"' + '\\' + static_cast<char>(0x01);

    const std::string json = message.to_json();
    EXPECT_NE(json.find("\\b"), std::string::npos);
    EXPECT_NE(json.find("\\f"), std::string::npos);
    EXPECT_NE(json.find("\\n"), std::string::npos);
    EXPECT_NE(json.find("\\r"), std::string::npos);
    EXPECT_NE(json.find("\\t"), std::string::npos);
    EXPECT_NE(json.find("\\\""), std::string::npos);
    EXPECT_NE(json.find("\\\\"), std::string::npos);
    EXPECT_NE(json.find("\\u0001"), std::string::npos);
}

TEST(ConnectorTests, MessageFromJsonAcceptsWhitespaceInBase64Payload) {
    const std::string with_ws =
        "{\"schema_version\":1,\"message_type\":\"telemetry_frame\",\"sequence\":1,\"timestamp_ms\":1,\"source\":\"simulated\",\"payload_b64\":\"A Q = =\\n\"}";
    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(with_ws);
    ASSERT_EQ(parsed.payload.size(), static_cast<std::size_t>(1));
    EXPECT_EQ(parsed.payload[0], 0x01);
}

TEST(ConnectorTests, MessageRoundTripCustomProtocolEnvelope) {
    connector::ConnectorMessage message;
    message.schema_version = 3;
    message.message_type = "telemetry_decoded";
    message.sequence = 999;
    message.timestamp_ms = 123;
    message.source = "decoder.flight-v2";
    message.payload = {0x41, 0x42};
    message.metadata["protocol"] = "lbr.flight.v2";

    const connector::ConnectorMessage parsed = connector::ConnectorMessage::from_json(message.to_json());
    EXPECT_EQ(parsed.schema_version, 3);
    EXPECT_EQ(parsed.message_type, std::string("telemetry_decoded"));
    EXPECT_EQ(parsed.source, std::string("decoder.flight-v2"));
    EXPECT_EQ(parsed.metadata.at("protocol"), std::string("lbr.flight.v2"));
}

TEST(ConnectorTests, NdjsonFileRoundTrip) {
    const std::filesystem::path file_path = temp_file_path("lbr_connector_roundtrip.ndjson");
    std::filesystem::remove(file_path);

    connector::ConnectorMessage message;
    message.sequence = 99;
    message.timestamp_ms = 1111;
    message.source = "simulated";
    message.payload = {0xAA, 0xBB, 0xCC};

    {
        connector::NdjsonFileWriter writer(file_path);
        writer.write(message);
    }

    {
        connector::NdjsonFileReader reader(file_path);
        connector::ConnectorMessage parsed;
        EXPECT_TRUE(reader.read_next(parsed));
        EXPECT_EQ(parsed.sequence, 99u);
        EXPECT_EQ(parsed.timestamp_ms, 1111);
        EXPECT_EQ(parsed.source, std::string("simulated"));
        EXPECT_EQ(parsed.payload, message.payload);
    }

    std::filesystem::remove(file_path);
}

TEST(ConnectorTests, NdjsonReaderSkipsEmptyLines) {
    const std::filesystem::path file_path = temp_file_path("lbr_connector_skip_empty.ndjson");
    std::filesystem::remove(file_path);

    {
        std::ofstream out(file_path, std::ios::binary);
        out << '\n';
        out << "{\"schema_version\":1,\"message_type\":\"telemetry_frame\",\"sequence\":4,\"timestamp_ms\":9,\"source\":\"simulated\",\"payload_b64\":\"AQ==\"}\n";
    }

    {
        connector::NdjsonFileReader reader(file_path);
        connector::ConnectorMessage parsed;
        EXPECT_TRUE(reader.read_next(parsed));
        EXPECT_EQ(parsed.sequence, 4u);
        EXPECT_FALSE(reader.read_next(parsed));
    }

    std::filesystem::remove(file_path);
}

TEST(ConnectorTests, NdjsonReaderReturnsFalseForEmptyFile) {
    const std::filesystem::path file_path = temp_file_path("lbr_connector_empty.ndjson");
    std::filesystem::remove(file_path);
    {
        std::ofstream out(file_path, std::ios::binary);
    }

    {
        connector::NdjsonFileReader reader(file_path);
        connector::ConnectorMessage parsed;
        EXPECT_FALSE(reader.read_next(parsed));
    }

    std::filesystem::remove(file_path);
}

TEST(ConnectorTests, NdjsonReaderReturnsFalseForWhitespaceOnlyFile) {
    const std::filesystem::path file_path = temp_file_path("lbr_connector_whitespace.ndjson");
    std::filesystem::remove(file_path);
    {
        std::ofstream out(file_path, std::ios::binary);
        out << "\n\n";
    }

    {
        connector::NdjsonFileReader reader(file_path);
        connector::ConnectorMessage parsed;
        EXPECT_FALSE(reader.read_next(parsed));
    }

    std::filesystem::remove(file_path);
}

TEST(ConnectorTests, NdjsonReaderThrowsForMissingFile) {
    const std::filesystem::path file_path = temp_file_path("lbr_connector_missing_file.ndjson");
    std::filesystem::remove(file_path);
    EXPECT_THROW(static_cast<void>(connector::NdjsonFileReader(file_path)), std::runtime_error);
}

TEST(ConnectorTests, NdjsonWriterThrowsForInvalidPath) {
    const std::filesystem::path invalid_path =
        std::filesystem::path("Z:/path/that/does/not/exist/lbr_connector.ndjson");
    EXPECT_THROW(static_cast<void>(connector::NdjsonFileWriter(invalid_path)), std::runtime_error);
}

TEST(ConnectorTests, LocalTcpTransportExchange) {
    constexpr std::uint16_t port = 45678;
    const std::string payload_line =
        connector::ConnectorMessage{1,
                                     "telemetry_frame",
                                     1,
                                     42,
                                     "simulated",
                                     {0x11, 0x22},
                                     std::nullopt,
                                     {}}.to_json();

    std::string received_line;

    std::thread server_thread([&received_line]() {
        connector::LocalTcpTransport server(connector::LocalTcpMode::Server,
                                            "127.0.0.1",
                                            port);
        server.open();
        ASSERT_TRUE(server.read_line(received_line));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalTcpTransport client(connector::LocalTcpMode::Client, "127.0.0.1", port);
    client.open();
    client.write_line(payload_line);

    server_thread.join();
    EXPECT_FALSE(received_line.empty());
    EXPECT_EQ(connector::ConnectorMessage::from_json(received_line).sequence, 1u);
}

TEST(ConnectorTests, LocalTcpTransportWriteBeforeOpenThrows) {
    connector::LocalTcpTransport transport(connector::LocalTcpMode::Client, "127.0.0.1", 45679);
    EXPECT_THROW(transport.write_line("hello"), std::runtime_error);
}

TEST(ConnectorTests, LocalTcpTransportReadBeforeOpenThrows) {
    connector::LocalTcpTransport transport(connector::LocalTcpMode::Client, "127.0.0.1", 45680);
    std::string line;
    EXPECT_THROW(static_cast<void>(transport.read_line(line)), std::runtime_error);
}

TEST(ConnectorTests, LocalTcpTransportRejectsInvalidHost) {
    connector::LocalTcpTransport transport(connector::LocalTcpMode::Client, "not-an-ip", 45681);
    EXPECT_THROW(static_cast<void>(transport.open()), std::runtime_error);
}

TEST(ConnectorTests, LocalTcpTransportClientConnectFailureThrows) {
    connector::LocalTcpTransport transport(connector::LocalTcpMode::Client, "127.0.0.1", 45999);
    EXPECT_THROW(static_cast<void>(transport.open()), std::runtime_error);
}

TEST(ConnectorTests, LocalTcpTransportReadReturnsFalseOnCleanCloseWithoutData) {
    constexpr std::uint16_t port = 45682;

    std::thread server_thread([port]() {
        connector::LocalTcpTransport server(connector::LocalTcpMode::Server, "127.0.0.1", port);
        server.open();
        server.close();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalTcpTransport client(connector::LocalTcpMode::Client, "127.0.0.1", port);
    client.open();
    std::string line;
    EXPECT_FALSE(client.read_line(line));

    server_thread.join();
}

TEST(ConnectorTests, LocalTcpTransportReturnsBufferedRemainderWhenPeerCloses) {
    constexpr std::uint16_t port = 45683;

    std::thread server_thread([port]() {
        connector::LocalTcpTransport server(connector::LocalTcpMode::Server, "127.0.0.1", port);
        server.open();
        server.write_line("partial");
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalTcpTransport client(connector::LocalTcpMode::Client, "127.0.0.1", port);
    client.open();
    std::string line;
    ASSERT_TRUE(client.read_line(line));
    EXPECT_EQ(line, "partial");

    server_thread.join();
}

TEST(ConnectorTests, LocalTcpTransportSupportsLocalhostAlias) {
    constexpr std::uint16_t port = 45684;
    std::string received;

    std::thread server_thread([&received, port]() {
        connector::LocalTcpTransport server(connector::LocalTcpMode::Server, "localhost", port);
        server.open();
        ASSERT_TRUE(server.read_line(received));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalTcpTransport client(connector::LocalTcpMode::Client, "localhost", port);
    client.open();
    client.write_line("hello");

    server_thread.join();
    EXPECT_EQ(received, "hello");
}

TEST(ConnectorTests, LocalTcpTransportSupportsEmptyHostAlias) {
    constexpr std::uint16_t port = 45685;
    std::string received;

    std::thread server_thread([&received, port]() {
        connector::LocalTcpTransport server(connector::LocalTcpMode::Server, "", port);
        server.open();
        ASSERT_TRUE(server.read_line(received));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalTcpTransport client(connector::LocalTcpMode::Client, "", port);
    client.open();
    client.write_line("");

    server_thread.join();
    EXPECT_EQ(received, "");
}

TEST(ConnectorTests, LocalUdpTransportExchange) {
    constexpr std::uint16_t port = 45690;
    const std::string payload =
        connector::ConnectorMessage{1,
                                    "telemetry_frame",
                                    22,
                                    123,
                                    "simulated",
                                    {0xDE, 0xAD, 0xBE, 0xEF},
                                    std::nullopt,
                                    {}}.to_json();

    std::string received;

    std::thread server_thread([&received, port]() {
        connector::LocalUdpTransport server(connector::LocalUdpMode::Server, "127.0.0.1", port);
        server.open();
        ASSERT_TRUE(server.read_datagram(received, 2000));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    connector::LocalUdpTransport client(connector::LocalUdpMode::Client, "127.0.0.1", port);
    client.open();
    client.write_datagram(payload);

    server_thread.join();

    ASSERT_FALSE(received.empty());
    EXPECT_EQ(connector::ConnectorMessage::from_json(received).sequence, 22u);
}

TEST(ConnectorTests, LocalUdpTransportReadTimesOutWithoutBlocking) {
    constexpr std::uint16_t port = 45691;

    connector::LocalUdpTransport server(connector::LocalUdpMode::Server, "127.0.0.1", port);
    server.open();

    std::string payload;
    const auto start = std::chrono::steady_clock::now();
    const bool has_data = server.read_datagram(payload, 100);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                               start)
            .count();

    EXPECT_FALSE(has_data);
    EXPECT_TRUE(payload.empty());
    EXPECT_GE(elapsed_ms, 50);
    EXPECT_LT(elapsed_ms, 1000);
}

TEST(ConnectorTests, LocalUdpTransportWriteBeforeOpenThrows) {
    connector::LocalUdpTransport transport(connector::LocalUdpMode::Client, "127.0.0.1", 45692);
    EXPECT_THROW(transport.write_datagram("hello"), std::runtime_error);
}

TEST(ConnectorTests, LocalUdpTransportReadBeforeOpenThrows) {
    connector::LocalUdpTransport transport(connector::LocalUdpMode::Client, "127.0.0.1", 45693);
    std::string payload;
    EXPECT_THROW(static_cast<void>(transport.read_datagram(payload, 5)), std::runtime_error);
}

TEST(ConnectorTests, LocalUdpTransportServerCannotSendBeforeLearningPeer) {
    connector::LocalUdpTransport server(connector::LocalUdpMode::Server, "127.0.0.1", 45694);
    server.open();
    EXPECT_THROW(server.write_datagram("payload"), std::runtime_error);
}

TEST(ConnectorTests, LocalZmqTransportThrowsWhenNotBuiltWithZmq) {
#if defined(LBR_HAS_ZEROMQ) && LBR_HAS_ZEROMQ
    SUCCEED();
#else
    connector::LocalZmqTransport publisher(connector::LocalZmqMode::Publisher,
                                           "tcp://127.0.0.1:5560",
                                           "telemetry");
    EXPECT_THROW(static_cast<void>(publisher.open()), std::runtime_error);
#endif
}

TEST(ConnectorTests, LocalZmqTransportPubSubExchange) {
#if defined(LBR_HAS_ZEROMQ) && LBR_HAS_ZEROMQ
    constexpr const char *endpoint = "tcp://127.0.0.1:5561";
    constexpr const char *topic = "telemetry";
    std::string received;

    std::thread subscriber_thread([&received]() {
        connector::LocalZmqTransport subscriber(connector::LocalZmqMode::Subscriber,
                                                endpoint,
                                                topic);
        subscriber.open();
        ASSERT_TRUE(subscriber.receive_message(received, 3000));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    connector::LocalZmqTransport publisher(connector::LocalZmqMode::Publisher,
                                           endpoint,
                                           topic);
    publisher.open();

    // Mitigate PUB/SUB slow-joiner timing variance on shared CI runners.
    for (int attempt = 0; attempt < 5; ++attempt) {
        publisher.send_message("payload-via-zmq");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    subscriber_thread.join();
    EXPECT_EQ(received, "payload-via-zmq");
#else
    GTEST_SKIP() << "ZeroMQ not enabled in this build.";
#endif
}
