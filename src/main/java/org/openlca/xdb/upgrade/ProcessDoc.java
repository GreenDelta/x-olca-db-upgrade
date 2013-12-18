package org.openlca.xdb.upgrade;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class ProcessDoc {

	@DbField("id")
	private String id;

	@DbField("processtype")
	private String processtype;

	@DbField("allocationmethod")
	private String allocationmethod;

	@DbField("infrastructureprocess")
	private String infrastructureprocess;

	@DbField("geographycomment")
	private String geographycomment;

	@DbField("description")
	private String description;

	@DbField("name")
	private String name;

	@DbField("categoryid")
	private String categoryid;

	@DbField("f_quantitativereference")
	private String f_quantitativereference;

	@DbField("f_location")
	private String f_location;

	@DbField("project")
	private String project;

	@DbField("creationdate")
	private Date creationdate;

	@DbField("intendedapplication")
	private String intendedapplication;

	@DbField("accessanduserestrictions")
	private String accessanduserestrictions;

	@DbField("copyright")
	private boolean copyright;

	@DbField("lastchange")
	private Date lastchange;

	@DbField("version")
	private String version;

	@DbField("f_datagenerator")
	private String f_datagenerator;

	@DbField("f_datasetowner")
	private String f_datasetowner;

	@DbField("f_datadocumentor")
	private String f_datadocumentor;

	@DbField("f_publication")
	private String f_publication;

	@DbField("modelingconstants")
	private String modelingconstants;

	@DbField("datatreatment")
	private String datatreatment;

	@DbField("sampling")
	private String sampling;

	@DbField("datacompleteness")
	private String datacompleteness;

	@DbField("datasetotherevaluation")
	private String datasetotherevaluation;

	@DbField("lcimethod")
	private String lcimethod;

	@DbField("datacollectionperiod")
	private String datacollectionperiod;

	@DbField("dataselection")
	private String dataselection;

	@DbField("f_reviewer")
	private String f_reviewer;

	@DbField("startdate")
	private Date startdate;

	@DbField("enddate")
	private Date enddate;

	@DbField("comment")
	private String comment;

	public static void map(IDatabase oldDb, IDatabase newDb, Sequence seq)
			throws Exception {
		String query = "SELECT * from tbl_processes p "
				+ "join tbl_admininfos a on p.id = a.id "
				+ "join tbl_modelingandvalidations m on m.id = a.id "
				+ "join tbl_technologies t on t.id = m.id "
				+ "join tbl_times i on i.id = t.id ";
		Mapper<ProcessDoc> mapper = new Mapper<>(ProcessDoc.class, oldDb, newDb);
		Handler handler = new Handler(seq);
		mapper.mapAll(query, handler);
	}

	private static class Handler extends UpdateHandler<ProcessDoc> {

		public Handler(Sequence seq) {
			super(seq);
		}

		@Override
		public String getStatement() {
			return "INSERT INTO tbl_process_docs(id, geography, "
					+ "technology, time, valid_from, valid_until, modeling_constants, "
					+ "data_treatment, sampling, completeness, review_details, "
					+ "inventory_method, data_collection_period, data_selection, "
					+ "f_reviewer, project, creation_date, intended_application, "
					+ "restrictions, copyright, last_change, version, f_data_generator, "
					+ "f_dataset_owner, f_data_documentor, f_publication) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, ?, ?)";
		}

		@Override
		protected void map(ProcessDoc doc, PreparedStatement stmt)
				throws SQLException {
			// id
			stmt.setInt(1, seq.get(Sequence.PROCESS, doc.id));
			// geography
			stmt.setString(2, doc.geographycomment);
			// technology
			stmt.setString(3, null);
			// time
			stmt.setString(4, doc.comment);
			// valid_from
			stmt.setDate(5, doc.startdate);
			// valid_until
			stmt.setDate(6, doc.enddate);
			// modeling_constants
			stmt.setString(7, doc.modelingconstants);
			// data_treatment
			stmt.setString(8, doc.datatreatment);
			// sampling
			stmt.setString(9, doc.sampling);
			// completeness
			stmt.setString(10, doc.datacompleteness);
			// review_details
			stmt.setString(11, doc.datasetotherevaluation);
			// inventory_method
			stmt.setString(12, doc.lcimethod);
			// data_collection_period
			stmt.setString(13, doc.datacollectionperiod);
			// data_selection
			stmt.setString(14, doc.dataselection);
			// f_reviewer
			if (doc.f_reviewer == null)
				stmt.setNull(15, java.sql.Types.INTEGER);
			else
				stmt.setInt(15, seq.get(Sequence.ACTOR, doc.f_reviewer));
			// project
			stmt.setString(16, doc.project);
			// creation_date
			stmt.setDate(17, doc.creationdate);
			// intended_application
			stmt.setString(18, doc.intendedapplication);
			// restrictions
			stmt.setString(19, doc.accessanduserestrictions);
			// copyright
			stmt.setBoolean(20, doc.copyright);
			// last_change
			stmt.setDate(21, doc.lastchange);
			// version
			stmt.setString(22, doc.version);
			// f_data_generator
			if (doc.f_datagenerator == null)
				stmt.setNull(23, java.sql.Types.INTEGER);
			else
				stmt.setInt(23, seq.get(Sequence.ACTOR, doc.f_datagenerator));
			// f_dataset_owner
			if (doc.f_datasetowner == null)
				stmt.setNull(24, java.sql.Types.INTEGER);
			else
				stmt.setInt(24, seq.get(Sequence.ACTOR, doc.f_datasetowner));
			// f_data_documentor
			if (doc.f_datadocumentor == null)
				stmt.setNull(25, java.sql.Types.INTEGER);
			else
				stmt.setInt(25, seq.get(Sequence.ACTOR, doc.f_datadocumentor));
			// f_publication
			if (doc.f_publication == null)
				stmt.setNull(26, java.sql.Types.INTEGER);
			else
				stmt.setInt(26, seq.get(Sequence.SOURCE, doc.f_publication));
		}
	}
}
