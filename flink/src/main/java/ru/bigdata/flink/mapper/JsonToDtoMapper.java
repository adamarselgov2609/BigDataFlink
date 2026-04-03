package ru.bigdata.flink.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.springframework.stereotype.Service;
import ru.bigdata.flink.dto.MockDataDto;

import java.io.IOException;

@Service
public class JsonToDtoMapper implements MapFunction<String, MockDataDto> {
    private static final ObjectMapper mapper = new ObjectMapper();

    public MockDataDto map(String json) throws IOException {
        return mapper.readValue(json, MockDataDto.class);
    }
}
