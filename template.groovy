// this is a script for creating the stubs of the mapping types


def type = "Process"
def var = "proc"

def oldFieldsStr = "id, processtype, allocationmethod, infrastructureprocess, geographycomment, description, name, categoryid, f_quantitativereference, f_location"
def newFieldsStr = "id, ref_id, name, f_category, description, process_type, default_allocation_method, infrastructure_process, f_quantitative_reference, f_location, f_process_doc"

def oldFields = oldFieldsStr.split(", ")
def newFields = newFieldsStr.split(", ")

def placeHolder = "?"
(newFields.size() - 1).times {
    placeHolder += ", ?"
}

println """
package org.openlca.xdb.upgrade;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

class $type {

    // TODO: check the field types
"""

oldFields.each { f ->

    println """
    @DbField("$f")
    private String $f;"""

}

println """
    public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
            throws Exception {
        // TODO: check the query + table name
        String query = "SELECT * FROM tbl_${type.toLowerCase()}s";
        Mapper<${type}> mapper = new Mapper<>(${type}.class);
        List<${type}> ${var}s = mapper.mapAll(oldDb, query);
        // TODO: check the query + table name
        String insertStmt = "INSERT INTO tbl_${type.toLowerCase()}s(${newFieldsStr}) "
                + "VALUES ($placeHolder)";
        Handler handler = new Handler(${var}s, seq);
        NativeSql.on(newDb).batchInsert(insertStmt, ${var}s.size(), handler);
    }
    
"""

println """

    private static class Handler extends UpdateHandler<$type> {

        public Handler(List<$type> ${var}s, Sequence seq) {
            super(${var}s, seq);
        }

        @Override
        protected void map($type ${var}, PreparedStatement stmt)
                throws SQLException {
                
        // TODO: new fields that need to be set in this order
"""
newFields.each { f ->
       println "\t\t // $f \n"
}

println "\t\t // prototypes for the mappings"

oldFields.eachWithIndex { f,i ->
    if(i == 0) {
        println "stmt.setInt(1, seq.get(Sequence.${type.toUpperCase()}, ${var}.${f}));"
        println "stmt.setString(2, ${var}.${f});"
     }
    else
        println "stmt.setString(${i+2}, ${var}.${f});"
}
println """ 

            // template for category references
            // if(Category.isNull(${var}.categoryId))
            //    stmt.setNull(#x, java.sql.Types.INTEGER);
            // else
            //    stmt.setInt(#x, seq.get(Sequence.CATEGORY, ${var}.categoryId));
            
            // template for optional references
            // if(${var}.optRefId == null) 
            //    stmt.setNull(#x, java.sql.Types.INTEGER);
            // else
            //    stmt.setInt(#x, seq.get(Sequence.REFTYPE, ${var}.optRefId));    
            
            // template for optional double fields
            // if(${var}.DOUBLE == null)
            // 	  stmt.setNull(#x, java.sql.Types.DOUBLE);
            // else
            // 	  stmt.setDouble(#x, ${var}.DOUBLE);
            
        }
    }
}
"""

