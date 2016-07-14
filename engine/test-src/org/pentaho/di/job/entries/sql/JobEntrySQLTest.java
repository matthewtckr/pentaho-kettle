/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.job.entries.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entries.sql.JobEntrySQL.SQLScript;
import org.pentaho.di.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class JobEntrySQLTest extends JobEntryLoadSaveTestSupport<JobEntrySQL> {

  @Override
  protected Class<JobEntrySQL> getJobEntryClass() {
    return JobEntrySQL.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( "Database", "SqlScripts" );
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> attrMap = new HashMap<>();
    attrMap.put( "SqlScripts",
      new ListLoadSaveValidator<SQLScript>( new SQLScriptFieldLoadSaveValidator(),
        new Random().nextInt( 5 ) ) );
    return attrMap;
  }

  static class SQLScriptFieldLoadSaveValidator implements FieldLoadSaveValidator<SQLScript> {
    static final Random rand = new Random();

    @Override
    public SQLScript getTestObject() {
      SQLScript object = new SQLScript();
      object.setSqlScript( UUID.randomUUID().toString() );
      object.setComments( UUID.randomUUID().toString() );
      object.setUseVariableSubstitution( rand.nextBoolean() );
      object.setReadFromFile( rand.nextBoolean() );
      object.setSplitScriptInFile( rand.nextBoolean() );
      return object;
    }

    @Override
    public boolean validateTestObject( SQLScript testObject, Object actual ) {
      if ( !( actual instanceof SQLScript ) ) {
        return false;
      }
      SQLScript actualScript = (SQLScript) actual;
      return testObject.getSqlScript().equals( actualScript.getSqlScript() )
        && testObject.getComments().equals( actualScript.getComments() )
        && testObject.isReadFromFile() == actualScript.isReadFromFile()
        && testObject.isSplitScriptInFile() == actualScript.isSplitScriptInFile()
        && testObject.isUseVariableSubstitution() == actualScript.isUseVariableSubstitution();
    }
  }

  private static Node getCompatibleXML(String query, String filename, boolean fromFile, boolean varSubstitute,
      boolean singleStatement, DatabaseMeta db ) throws KettleXMLException {
    StringBuilder xml = new StringBuilder();
    xml.append( "    " ).append( XMLHandler.openTag( "entry" ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "sql", query ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "useVariableSubstitution", varSubstitute ? "T" : "F" ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "sqlfromfile", fromFile ? "T" : "F" ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "sqlfilename", filename ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "sendOneStatement", singleStatement ? "T" : "F" ) );
    xml.append( "      " ).append( XMLHandler.addTagValue( "connection", db.getName() ) );
    xml.append( "    " ).append( XMLHandler.closeTag( "entry" ) );

    Document doc = XMLHandler.loadXMLString( xml.toString() );
    Node entrynode = XMLHandler.getSubNode( doc, "entry" );
    return entrynode;
  }

  @Test
  public void testXMLBackwardsCompatibility() throws KettleException {
    List<DatabaseMeta> databases = new ArrayList<>();
    DatabaseMeta testDb = mock( DatabaseMeta.class );
    when( testDb.getName() ).thenReturn( "testdb" );
    databases.add( testDb );

    String sql = "SELECT * FROM users";
    Node entrynode = getCompatibleXML( sql, null, false, false, false, testDb );
    JobEntrySQL meta = new JobEntrySQL();
    meta.loadXML( entrynode, databases, null, null, null );
    assertEquals( testDb, meta.getDatabase() );
    assertEquals( 1, meta.getSqlScripts().size() );
    assertEquals( "SELECT * FROM users", meta.getSqlScripts().get( 0 ).getSqlScript() );
    assertTrue( Const.isEmpty( meta.getSqlScripts().get( 0 ).getComments() ) );
    assertEquals( false, meta.getSqlScripts().get( 0 ).isUseVariableSubstitution() );
    assertEquals( false, meta.getSqlScripts().get( 0 ).isReadFromFile() );
    assertEquals( false, meta.getSqlScripts().get( 0 ).isSplitScriptInFile() );

    // Also test that a trailing semicolon doesn't create extra queries
    sql = "SELECT * FROM users;SELECT * FROM contacts;";
    entrynode = getCompatibleXML( sql, null, false, false, false, testDb );
    meta = new JobEntrySQL();
    meta.loadXML( entrynode, databases, null, null, null );
    assertEquals( testDb, meta.getDatabase() );
    assertEquals( 2, meta.getSqlScripts().size() );
    assertEquals( "SELECT * FROM users", meta.getSqlScripts().get( 0 ).getSqlScript() );
    assertTrue( Const.isEmpty( meta.getSqlScripts().get( 0 ).getComments() ) );
    assertFalse( meta.getSqlScripts().get( 0 ).isUseVariableSubstitution() );
    assertFalse( meta.getSqlScripts().get( 0 ).isReadFromFile() );
    assertFalse( meta.getSqlScripts().get( 0 ).isSplitScriptInFile() );
    assertEquals( "SELECT * FROM contacts", meta.getSqlScripts().get( 1 ).getSqlScript() );
    assertTrue( Const.isEmpty( meta.getSqlScripts().get( 1 ).getComments() ) );
    assertFalse( meta.getSqlScripts().get( 1 ).isUseVariableSubstitution() );
    assertFalse( meta.getSqlScripts().get( 1 ).isReadFromFile() );
    assertFalse( meta.getSqlScripts().get( 1 ).isSplitScriptInFile() );

    // Test SQL from file
    sql = "C:\\JobEntrySQLTest_notarealfile.sql";
    entrynode = getCompatibleXML( null, sql, true, true, false, testDb );
    meta = new JobEntrySQL();
    meta.loadXML( entrynode, databases, null, null, null );
    assertEquals( testDb, meta.getDatabase() );
    assertEquals( 1, meta.getSqlScripts().size() );
    assertEquals( sql, meta.getSqlScripts().get( 0 ).getSqlScript() );
    assertTrue( Const.isEmpty( meta.getSqlScripts().get( 0 ).getComments() ) );
    assertTrue( meta.getSqlScripts().get( 0 ).isUseVariableSubstitution() );
    assertTrue( meta.getSqlScripts().get( 0 ).isReadFromFile() );
    assertTrue( meta.getSqlScripts().get( 0 ).isSplitScriptInFile() );

    // Test SQL from file and single statement
    sql = "C:\\JobEntrySQLTest_notarealfile.sql";
    entrynode = getCompatibleXML( null, sql, true, true, true, testDb );
    meta = new JobEntrySQL();
    meta.loadXML( entrynode, databases, null, null, null );
    assertEquals( testDb, meta.getDatabase() );
    assertEquals( 1, meta.getSqlScripts().size() );
    assertEquals( sql, meta.getSqlScripts().get( 0 ).getSqlScript() );
    assertTrue( Const.isEmpty( meta.getSqlScripts().get( 0 ).getComments() ) );
    assertTrue( meta.getSqlScripts().get( 0 ).isUseVariableSubstitution() );
    assertTrue( meta.getSqlScripts().get( 0 ).isReadFromFile() );
    assertFalse( meta.getSqlScripts().get( 0 ).isSplitScriptInFile() );
  }
}
