package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SourceTable {
	private String syString;
	private String db_sidString;
	private String table_schemaString;
	private String table_nameString;
	private String table_cn_nameString;
	private String inc_cdtString;
	private String if_markString;
	private String table_typeString;
	private String template_codeString;
	private String is_put_to_etldbString;
	
	
	public SourceTable() {
		super();
	}
	
	@Override
	public String toString() {
		return "SourceTable [syString=" + syString + ", db_sidString=" + db_sidString + ", table_schemaString="
				+ table_schemaString + ", table_nameString=" + table_nameString + ", table_cn_nameString="
				+ table_cn_nameString + ", inc_cdtString=" + inc_cdtString + ", if_markString=" + if_markString
				+ ", table_typeString=" + table_typeString + ", template_codeString=" + template_codeString
				+ ", is_put_to_etldbString=" + is_put_to_etldbString + "]";
	}
	
	
	public String getSyString() {
		return syString;
	}
	public void setSyString(String syString) {
		this.syString = syString;
	}

	public String getDb_sidString() {
		return db_sidString;
	}
	public void setDb_sidString(String db_sidString) {
		this.db_sidString = db_sidString;
	}
	
	public String getTable_schemaString() {
		return table_schemaString;
	}
	public void setTable_schemaString(String table_schemaString) {
		this.table_schemaString = table_schemaString;
	}
	public String getTable_nameString() {
		return table_nameString;
	}

	public void setTable_nameString(String table_nameString) {
		this.table_nameString = table_nameString;
	}

	
	

	public SourceTable(String syString, String db_sidString, String table_schemaString, String table_nameString,
			String table_cn_nameString, String inc_cdtString, String if_markString, String table_typeString,
			String template_codeString, String is_put_to_etldbString) {
		super();
		this.syString = syString;
		this.db_sidString = db_sidString;
		this.table_schemaString = table_schemaString;
		this.table_nameString = table_nameString;
		this.table_cn_nameString = table_cn_nameString;
		this.inc_cdtString = inc_cdtString;
		this.if_markString = if_markString;
		this.table_typeString = table_typeString;
		this.template_codeString = template_codeString;
		this.is_put_to_etldbString = is_put_to_etldbString;
	}
	

	
	public String getTable_cn_nameString() {
		return table_cn_nameString;
	}
	public void setTable_cn_nameString(String table_cn_nameString) {
		this.table_cn_nameString = table_cn_nameString;
	}
	public String getInc_cdtString() {
		return inc_cdtString;
	}
	public void setInc_cdtString(String inc_cdtString) {
		this.inc_cdtString = inc_cdtString;
	}
	public String getIf_markString() {
		return if_markString;
	}
	public void setIf_markString(String if_markString) {
		this.if_markString = if_markString;
	}
	public String getTable_typeString() {
		return table_typeString;
	}
	public void setTable_typeString(String table_typeString) {
		this.table_typeString = table_typeString;
	}
	public String getTemplate_codeString() {
		return template_codeString;
	}
	public void setTemplate_codeString(String template_codeString) {
		this.template_codeString = template_codeString;
	}
	public String getIs_put_to_etldbString() {
		return is_put_to_etldbString;
	}
	public void setIs_put_to_etldbString(String is_put_to_etldbString) {
		this.is_put_to_etldbString = is_put_to_etldbString;
	}
	
	
}
