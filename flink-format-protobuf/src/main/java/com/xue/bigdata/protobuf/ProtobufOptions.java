package com.xue.bigdata.protobuf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author: mingway
 * @date: 2022/8/12 10:54 PM
 */
public class ProtobufOptions {

    public static final ConfigOption<String> PROTOBUF_CLASS_NAME =
            ConfigOptions.key("class-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<String> PROTOBUF_DESCRIPTOR_FILE =
            ConfigOptions.key("descriptor-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");
}
