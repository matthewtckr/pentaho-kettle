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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.SqlScriptParser;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.job.entry.validator.AndValidator;
import org.pentaho.di.job.entry.validator.JobEntryValidatorUtils;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceEntry.ResourceType;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * This defines an SQL job entry.
 *
 * @author Matt
 * @since 05-11-2003
 *
 */
public class JobEntrySQL extends JobEntryBase implements Cloneable, JobEntryInterface {
  private static Class<?> PKG = JobEntrySQL.class; // for i18n purposes, needed by Translator2!!

  private DatabaseMeta connection;
  private List<SQLScript> sqlScripts;

  public JobEntrySQL( String n ) {
    super( n, "" );
    connection = null;
    sqlScripts = new ArrayList<>();
  }

  public JobEntrySQL() {
    this( "" );
  }

  @Override
  public Object clone() {
    JobEntrySQL je = (JobEntrySQL) super.clone();
    je.sqlScripts = new ArrayList<SQLScript>();
    for ( SQLScript script : sqlScripts ) {
      je.sqlScripts.add( script.clone() );
    }
    return je;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( super.getXML() );

    retval.append( "      " ).append(
      XMLHandler.addTagValue( "connection", connection == null ? null : connection.getName() ) );

    retval.append( "      " ).append( XMLHandler.openTag( "scripts" ) ).append( Const.CR );
    for ( int i = 0; i < sqlScripts.size(); i++ ) {
      SQLScript script = sqlScripts.get( i );
      retval.append( "        " ).append( XMLHandler.openTag( "script" ) ).append( Const.CR );
      retval.append( "          " ).append(
        XMLHandler.addTagValue( "sqlScript", script.getSqlScript() ) );
      retval.append( "          " ).append(
        XMLHandler.addTagValue( "sqlComment", script.getComments() ) );
      retval.append( "          " ).append(
        XMLHandler.addTagValue( "sqlVariableSubstitution", script.isUseVariableSubstitution() ) );
      retval.append( "          " ).append(
        XMLHandler.addTagValue( "sqlReadFromFile", script.isReadFromFile() ) );
      retval.append( "          " ).append(
        XMLHandler.addTagValue( "sqlSplitScriptInFile", script.isSplitScriptInFile() ) );
      retval.append( "        " ).append( XMLHandler.closeTag( "script" ) ).append( Const.CR );
    }
    retval.append( "      " ).append( XMLHandler.closeTag( "scripts" ) ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
    Repository rep, IMetaStore metaStore ) throws KettleXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );

      String dbname = XMLHandler.getTagValue( entrynode, "connection" );
      connection = DatabaseMeta.findDatabase( databases, dbname );

      boolean oldXMLFormat = XMLHandler.countNodes( entrynode, "sql" ) > 0
        || XMLHandler.countNodes( entrynode, "sqlfromfile" ) > 0;
      if ( oldXMLFormat ) {
        boolean useVariableSubstitution = false;
        boolean sendOneStatement = false;
        String sSubs = XMLHandler.getTagValue( entrynode, "useVariableSubstitution" );
        if ( sSubs != null && sSubs.equalsIgnoreCase( "T" ) ) {
          useVariableSubstitution = true;
        }

        String sOneStatement = XMLHandler.getTagValue( entrynode, "sendOneStatement" );
        if ( sOneStatement != null && sOneStatement.equalsIgnoreCase( "T" ) ) {
          sendOneStatement = true;
        }

        String ssql = XMLHandler.getTagValue( entrynode, "sqlfromfile" );
        if ( ssql != null && ssql.equalsIgnoreCase( "T" ) ) {
          String sqlfilename = XMLHandler.getTagValue( entrynode, "sqlfilename" );
          SQLScript scriptEntry = new SQLScript();
          scriptEntry.setReadFromFile( true );
          scriptEntry.setSqlScript( sqlfilename );
          scriptEntry.setUseVariableSubstitution( useVariableSubstitution );
          scriptEntry.setSplitScriptInFile( !sendOneStatement );
          sqlScripts.add( scriptEntry );
        } else {
          String sql = XMLHandler.getTagValue( entrynode, "sql" );

          List<String> sqlStatements = null;
          if ( sendOneStatement ) {
            sqlStatements = Arrays.asList( new String[]{ sql } );
          } else {
            sqlStatements = SqlScriptParser.getInstance().split( sql );
          }
          for ( String sqlStatement : sqlStatements ) {
            SQLScript scriptEntry = new SQLScript();
            scriptEntry.setSqlScript( sqlStatement );
            scriptEntry.setUseVariableSubstitution( useVariableSubstitution );
            sqlScripts.add( scriptEntry );
          }
        }
      } else {
        Node scriptsNode = XMLHandler.getSubNode( entrynode, "scripts" );
        int scriptNodes = XMLHandler.countNodes( scriptsNode, "script" );
        for ( int i = 0; i < scriptNodes; i++ ) {
          Node scriptNode = XMLHandler.getSubNodeByNr( scriptsNode, "script", i );
          SQLScript script = new SQLScript();
          script.setSqlScript( XMLHandler.getTagValue( scriptNode, "sqlScript" ) );
          script.setComments( XMLHandler.getTagValue( scriptNode, "sqlComment" ) );
          script.setUseVariableSubstitution(
            "Y".equalsIgnoreCase( XMLHandler.getTagValue( scriptNode, "sqlVariableSubstitution" ) ) );
          script.setReadFromFile(
            "Y".equalsIgnoreCase( XMLHandler.getTagValue( scriptNode, "sqlReadFromFile" ) ) );
          script.setSplitScriptInFile(
            "Y".equalsIgnoreCase( XMLHandler.getTagValue( scriptNode, "sqlSplitScriptInFile" ) ) );
          sqlScripts.add( script );
        }
      }
    } catch ( KettleException e ) {
      throw new KettleXMLException( "Unable to load job entry of type 'sql' from XML node", e );
    }
  }

  @Override
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
    List<SlaveServer> slaveServers ) throws KettleException {
    try {
      connection = rep.loadDatabaseMetaFromJobEntryAttribute( id_jobentry, "connection", "id_database", databases );

      boolean oldRepoFormat = rep.countNrJobEntryAttributes( id_jobentry, "sql" ) > 0
          || rep.countNrJobEntryAttributes( id_jobentry, "sqlfromfile" ) > 0;
      if ( oldRepoFormat ) {
        boolean useVariableSubstitution = false;
        boolean sendOneStatement = false;
        String sSubs = rep.getJobEntryAttributeString( id_jobentry, "useVariableSubstitution" );
        if ( sSubs != null && sSubs.equalsIgnoreCase( "T" ) ) {
          useVariableSubstitution = true;
        }

        String ssendOneStatement = rep.getJobEntryAttributeString( id_jobentry, "sendOneStatement" );
        if ( ssendOneStatement != null && ssendOneStatement.equalsIgnoreCase( "T" ) ) {
          sendOneStatement = true;
        }

        String ssql = rep.getJobEntryAttributeString( id_jobentry, "sqlfromfile" );
        if ( ssql != null && ssql.equalsIgnoreCase( "T" ) ) {
          String sqlfilename = rep.getJobEntryAttributeString( id_jobentry, "sqlfilename" );
          SQLScript scriptEntry = new SQLScript();
          scriptEntry.setReadFromFile( true );
          scriptEntry.setSqlScript( sqlfilename );
          scriptEntry.setUseVariableSubstitution( useVariableSubstitution );
          scriptEntry.setSplitScriptInFile( !sendOneStatement );
          sqlScripts.add( scriptEntry );
        } else {
          String sql = rep.getJobEntryAttributeString( id_jobentry, "sql" );
          List<String> sqlStatements = null;
          if ( sendOneStatement ) {
            sqlStatements = Arrays.asList( new String[]{ sql } );
          } else {
            sqlStatements = SqlScriptParser.getInstance().split( sql );
          }
          for ( String sqlStatement : sqlStatements ) {
            SQLScript scriptEntry = new SQLScript();
            scriptEntry.setSqlScript( sqlStatement );
            scriptEntry.setUseVariableSubstitution( useVariableSubstitution );
            sqlScripts.add( scriptEntry );
          }
        }
      } else {
        int scriptEntries = rep.countNrJobEntryAttributes( id_jobentry, "sqlScript" );
        for ( int i = 0; i < scriptEntries; i++ ) {
          SQLScript script = new SQLScript();
          script.setSqlScript(
            rep.getJobEntryAttributeString( id_jobentry, i, "sqlScript" ) );
          script.setComments(
            rep.getJobEntryAttributeString( id_jobentry, i, "sqlComment" ) );
          script.setUseVariableSubstitution(
            rep.getJobEntryAttributeBoolean( id_jobentry, i, "sqlVariableSubstitution" ) );
          script.setReadFromFile(
            rep.getJobEntryAttributeBoolean( id_jobentry, i, "sqlReadFromFile" ) );
          script.setSplitScriptInFile(
            rep.getJobEntryAttributeBoolean( id_jobentry, i, "sqlSplitScriptInFile" ) );
          sqlScripts.add( script );
        }
      }
    } catch ( KettleDatabaseException dbe ) {
      throw new KettleException( "Unable to load job entry of type 'sql' from the repository with id_jobentry="
        + id_jobentry, dbe );
    }
  }

  // Save the attributes of this job entry
  //
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    try {
      rep.saveDatabaseMetaJobEntryAttribute( id_job, getObjectId(), "connection", "id_database", connection );

      for ( int i = 0; i < sqlScripts.size(); i++ ) {
        SQLScript script = sqlScripts.get( i );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "sqlScript", script.getSqlScript() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "sqlComment", script.getComments() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "sqlVariableSubstitution",
          script.isUseVariableSubstitution() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "sqlReadFromFile",
          script.isReadFromFile() );
        rep.saveJobEntryAttribute( id_job, getObjectId(), i, "sqlSplitScriptInFile",
          script.isSplitScriptInFile() );
      }
    } catch ( KettleDatabaseException dbe ) {
      throw new KettleException(
        "Unable to save job entry of type 'sql' to the repository for id_job=" + id_job, dbe );
    }
  }

  public void setDatabase( DatabaseMeta database ) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return connection;
  }

  public List<SQLScript> getSqlScripts() {
    return sqlScripts;
  }

  public void setSqlScripts( List<SQLScript> sqlScripts ) {
    this.sqlScripts = sqlScripts;
  }

  @Override
  public Result execute( Result previousResult, int nr ) {
    Result result = previousResult;

    if ( connection != null ) {
      Database db = new Database( this, connection );

      db.shareVariablesWith( this );
      try {
        String mySQL = null;
        db.connect( parentJob.getTransactionId(), null );

        List<SQLScript> finalScripts = new ArrayList<SQLScript>();
        for ( SQLScript script : sqlScripts ) {
          if ( script.isReadFromFile() ) {
            if ( script.getSqlScript() == null ) {
              throw new KettleDatabaseException( BaseMessages.getString( PKG, "JobSQL.NoSQLFileSpecified" ) );
            }
            FileObject SQLfile = null;
            try {
              String realfilename = environmentSubstitute( script.getSqlScript() );
              SQLfile = KettleVFS.getFileObject( realfilename, this );
              if ( !SQLfile.exists() ) {
                logError( BaseMessages.getString( PKG, "JobSQL.SQLFileNotExist", realfilename ) );
                throw new KettleDatabaseException( BaseMessages.getString(
                  PKG, "JobSQL.SQLFileNotExist", realfilename ) );
              }
              if ( isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JobSQL.SQLFileExists", realfilename ) );
              }

              InputStream IS = KettleVFS.getInputStream( SQLfile );
              InputStreamReader BIS = null;
              BufferedReader buff = null;
              try {
                BIS = new InputStreamReader( new BufferedInputStream( IS, 500 ) );

                buff = new BufferedReader( BIS );
                String sLine = null;
                mySQL = Const.CR;

                while ( ( sLine = buff.readLine() ) != null ) {
                  if ( Const.isEmpty( sLine ) ) {
                    mySQL = mySQL + Const.CR;
                  } else {
                    mySQL = mySQL + Const.CR + sLine;
                  }
                }
              } finally {
                if ( buff != null ) {
                  try {
                    buff.close();
                  } catch ( Exception e ) {
                    // Ignore errors
                  }
                }
                if ( BIS != null ) {
                  try {
                    BIS.close();
                  } catch ( Exception e ) {
                    // Ignore errors
                  }
                }
                if ( IS != null ) {
                  try {
                    IS.close();
                  } catch ( Exception e ) {
                    // Ignore errors
                  }
                }
                if ( SQLfile != null ) {
                  try {
                    SQLfile.close();
                  } catch ( Exception e ) {
                    // Ignore errors
                  }
                }
              }
            } catch ( Exception e ) {
              throw new KettleDatabaseException( BaseMessages.getString( PKG, "JobSQL.ErrorRunningSQLfromFile" ), e );
            }

            if ( script.isSplitScriptInFile() ) {
              List<String> sqlStatements = SqlScriptParser.getInstance().split( mySQL );
              for ( String sql : sqlStatements ) {
                SQLScript newScript = script.clone();
                newScript.setSqlScript( sql );
                newScript.setReadFromFile( false );
                finalScripts.add( newScript );
              }
              script = null;
            } else {
              script.setSqlScript( mySQL );
              script.setReadFromFile( false );
            }
          }
          if ( script != null ) {
            finalScripts.add( script );
          }
        }

        for ( SQLScript script : finalScripts ) {
          if ( !Const.isEmpty( script.getSqlScript() ) ) {
            String sql = null;
            if ( script.isUseVariableSubstitution() ) {
              sql = environmentSubstitute( script.getSqlScript() );
            }
            if ( isDetailed() ) {
              logDetailed( BaseMessages.getString( PKG, "JobSQL.Log.SQlStatement", mySQL ) );
            }
            db.execStatement( sql );
          }
        }
      } catch ( KettleDatabaseException je ) {
        result.setNrErrors( 1 );
        logError( BaseMessages.getString( PKG, "JobSQL.ErrorRunJobEntry", je.getMessage() ) );
      } finally {
        db.disconnect();
      }
    } else {
      result.setNrErrors( 1 );
      logError( BaseMessages.getString( PKG, "JobSQL.NoDatabaseConnection" ) );
    }

    if ( result.getNrErrors() == 0 ) {
      result.setResult( true );
    } else {
      result.setResult( false );
    }

    return result;
  }

  @Override
  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] { connection, };
  }

  @Override
  public List<ResourceReference> getResourceDependencies( JobMeta jobMeta ) {
    List<ResourceReference> references = super.getResourceDependencies( jobMeta );
    if ( connection != null ) {
      ResourceReference reference = new ResourceReference( this );
      reference.getEntries().add( new ResourceEntry( connection.getHostname(), ResourceType.SERVER ) );
      reference.getEntries().add( new ResourceEntry( connection.getDatabaseName(), ResourceType.DATABASENAME ) );
      references.add( reference );
    }
    return references;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    JobEntryValidatorUtils.andValidator().validate( this, "SQL", remarks,
      AndValidator.putValidators( JobEntryValidatorUtils.notBlankValidator() ) );
  }

  public static class SQLScript implements Cloneable {
    private String sqlScript;
    private String comments;
    private boolean useVariableSubstitution;
    private boolean readFromFile;
    private boolean splitScriptInFile;

    public String getSqlScript() {
      return sqlScript;
    }

    @Override
    protected SQLScript clone() {
      try {
        return (SQLScript) super.clone();
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( e );
      }
    }

    public void setSqlScript( String sqlScript ) {
      this.sqlScript = sqlScript;
    }
    public String getComments() {
      return comments;
    }
    public void setComments( String comments ) {
      this.comments = comments;
    }
    public boolean isUseVariableSubstitution() {
      return useVariableSubstitution;
    }
    public void setUseVariableSubstitution( boolean useVariableSubstitution ) {
      this.useVariableSubstitution = useVariableSubstitution;
    }
    public boolean isReadFromFile() {
      return readFromFile;
    }
    public void setReadFromFile( boolean readFromFile ) {
      this.readFromFile = readFromFile;
    }
    public boolean isSplitScriptInFile() {
      return splitScriptInFile;
    }
    public void setSplitScriptInFile( boolean splitScriptInFile ) {
      this.splitScriptInFile = splitScriptInFile;
    }

  }
}
