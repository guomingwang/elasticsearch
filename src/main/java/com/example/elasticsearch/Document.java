package com.example.elasticsearch;

import lombok.Builder;
import lombok.Data;

/**
 * @author wgm
 * @since 2021/4/28
 */
@Data
@Builder
public class Document {

    private String id;
    private String date;

}
