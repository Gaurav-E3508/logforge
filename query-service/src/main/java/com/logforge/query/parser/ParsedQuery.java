package com.logforge.query.parser;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;

@Data
@AllArgsConstructor
public class ParsedQuery {

    private List<QueryClause> clauses;
    private Operator          operator;
    private String            rawQuery;

    public enum Operator { AND, OR }

    public static ParsedQuery empty() {
        return new ParsedQuery(List.of(), Operator.AND, "");
    }

    public boolean isEmpty() { return clauses.isEmpty(); }
}