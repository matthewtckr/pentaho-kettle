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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.pentaho.di.job.entries.sql.JobEntrySQL.SQLScript;
import org.pentaho.di.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;

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
}
