package com.moumou.bootmybatisdemo.dataAlignment.model;

public class SourceField {
	private String sysString;
	private String db_sidString;
	private String db_schemaString;
	private String table_nameString;
	private int column_id;
	private String column_nameString;
	private String column_typeString;
	private String column_cn_nameString;
	private String is_pkString;
	private String not_nullString;
	private String default_valueString;
	private String is_dkString;
	private String break_flag;
	private String table_cn_nameString;	//表中文名

	
	public SourceField() {
		super();
	}
	

	public String getTable_cn_nameString() {
		return table_cn_nameString;
	}


	public void setTable_cn_nameString(String table_cn_nameString) {
		this.table_cn_nameString = table_cn_nameString;
	}



	public String getBreak_flag() {
		return break_flag;
	}






	public void setBreak_flag(String break_flag) {
		this.break_flag = break_flag;
	}






	public SourceField(String sysString, String db_sidString, String db_schemaString, String table_nameString,
			int column_id, String column_nameString, String column_typeString, String column_cn_nameString,
			String is_pkString, String not_nullString, String default_valueString, String is_dkString,
			String break_flag) {
		super();
		this.sysString = sysString;
		this.db_sidString = db_sidString;
		this.db_schemaString = db_schemaString;
		this.table_nameString = table_nameString;
		this.column_id = column_id;
		this.column_nameString = column_nameString;
		this.column_typeString = column_typeString;
		this.column_cn_nameString = column_cn_nameString;
		this.is_pkString = is_pkString;
		this.not_nullString = not_nullString;
		this.default_valueString = default_valueString;
		this.is_dkString = is_dkString;
		this.break_flag = break_flag;
	}


	public String getSysString() {
		return sysString;
	}
	public void setSysString(String sysString) {
		this.sysString = sysString;
	}
	
	public String getDb_sidString() {
		return db_sidString;
	}


	public void setDb_sidString(String db_sidString) {
		this.db_sidString = db_sidString;
	}


	public String getColumn_nameString() {
		return column_nameString;
	}


	public void setColumn_nameString(String column_nameString) {
		this.column_nameString = column_nameString;
	}


	public String getDb_schemaString() {
		return db_schemaString;
	}
	public void setDb_schemaString(String db_schemaString) {
		this.db_schemaString = db_schemaString;
	}
	public String getTable_nameString() {
		return table_nameString;
	}
	public void setTable_nameString(String table_nameString) {
		this.table_nameString = table_nameString;
	}
	public int getColumn_id() {
		return column_id;
	}
	public void setColumn_id(int column_id) {
		this.column_id = column_id;
	}
	public String getColumn_naString() {
		return column_nameString;
	}
	public void setColumn_naString(String column_nameString) {
		this.column_nameString = column_nameString;
	}
	public String getColumn_typeString() {
		return column_typeString;
	}
	public void setColumn_typeString(String column_typeString) {
		this.column_typeString = column_typeString;
	}
	public String getColumn_cn_nameString() {
		return column_cn_nameString;
	}
	public void setColumn_cn_nameString(String column_cn_nameString) {
		this.column_cn_nameString = column_cn_nameString;
	}
	public String getIs_pkString() {
		return is_pkString;
	}
	public void setIs_pkString(String is_pkString) {
		this.is_pkString = is_pkString;
	}
	public String getNot_nullString() {
		return not_nullString;
	}
	public void setNot_nullString(String not_nullString) {
		this.not_nullString = not_nullString;
	}
	public String getDefault_valueString() {
		return default_valueString;
	}
	public void setDefault_valueString(String default_valueString) {
		this.default_valueString = default_valueString;
	}
	public String getIs_dkString() {
		return is_dkString;
	}
	public void setIs_dkString(String is_dkString) {
		this.is_dkString = is_dkString;
	}


	@Override
	public String toString() {
		return "SourceField [sysString=" + sysString + ", db_sidString=" + db_sidString + ", db_schemaString="
				+ db_schemaString + ", table_nameString=" + table_nameString + ", column_id=" + column_id
				+ ", column_nameString=" + column_nameString + ", column_typeString=" + column_typeString
				+ ", column_cn_nameString=" + column_cn_nameString + ", is_pkString=" + is_pkString
				+ ", not_nullString=" + not_nullString + ", default_valueString=" + default_valueString
				+ ", is_dkString=" + is_dkString + ", break_flag=" + break_flag + "]";
	}



}
