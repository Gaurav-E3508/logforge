package com.logforge.query.parser;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class QueryClause {
    private Type   type;
    private String field;   // "level", "service", "message", "category"
    private String value;

    public enum Type { FIELD, TEXT, PHRASE }
}