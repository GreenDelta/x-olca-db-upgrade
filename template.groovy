


def type = "Exchange"
def var = "exchange"

/*
def fields = "id, f_owner, f_flow, f_unit, is_input, f_flow_property_factor, resulting_amount_value, resulting_amount_formula, avoided_product, f_default_provider, distribution_type, parameter1_value, parameter1_formula, parameter2_value, parameter2_formula, parameter3_value, parameter3_formula, pedigree_uncertainty, base_uncertainty"
fields.split(", ").each {f ->
    println "//$f"
}
*/


def fields = [
"id", "avoidedproduct", "distributionType", "input", "f_flowpropertyfactor", 
"f_unit", "f_flow", "parametrized", "resultingamount_value", "resultingamount_formula", 
"parameter1_value", "parameter1_formula", "parameter2_value", "parameter2_formula", 
"parameter3_value", "parameter3_formula", "f_owner", "pedigree_uncertainty", 
"base_uncertainty", "f_default_provider"
]

println """
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.openlca.core.database.IDatabase;
import org.openlca.core.database.NativeSql;

class $type {
"""

fields.each { f ->

    println """
    @DbField("$f")
    private String $f;"""

}

println """
    public static void map(OldDatabase oldDb, IDatabase newDb, Sequence seq)
            throws Exception {
        String query = "SELECT * FROM tbl_${type.toLowerCase()}s";
        Mapper<${type}> mapper = new Mapper<>(${type}.class);
        List<${type}> ${var}s = mapper.mapAll(oldDb, query);
        String insertStmt = "INSERT INTO tbl_${type.toLowerCase()}s(id, ref_id, name, "
                + "description, model_type, f_parent_category) "
                + "VALUES (?, ?, ?, ?, ?, ?)";
        Handler handler = new Handler(${var}s, seq);
        NativeSql.on(newDb).batchInsert(insertStmt, ${var}s.size(), handler);
    }
    
"""

println """

    private static class Handler extends AbstractInsertHandler<$type> {

        public Handler(List<$type> ${var}s, Sequence seq) {
            super(${var}s, seq);
        }

        @Override
        protected void map($type ${var}, PreparedStatement stmt)
                throws SQLException {
"""
fields.eachWithIndex { f,i ->
    if(i == 0) {
        println "stmt.setInt(1, seq.get(Sequence.${type.toUpperCase()}, ${var}.${f}));"
        println "stmt.setString(2, ${var}.${f});"
     }
    else
        println "stmt.setString(${i+2}, ${var}.${f});"
}
println """
            if(Category.isNull(${var}.categoryId))
                stmt.setNull(#x, java.sql.Types.INTEGER);
            else
                stmt.setInt(#x, seq.get(Sequence.CATEGORY, ${var}.categoryId));

        }
    }
}
"""

