/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.job.entries.sql;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.sql.JobEntrySQL;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * This dialog allows you to edit the SQL job entry settings. (select the connection and the sql script to be executed)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class JobEntrySQLDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntrySQL.class; // for i18n purposes, needed by Translator2!!

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobSQL.Filetype.Sql" ), BaseMessages.getString( PKG, "JobSQL.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobSQL.Filetype.All" ) };

  private static final String[] yesNoOptions = new String[]{ BaseMessages.getString( "System.Button.Yes" ),
    BaseMessages.getString( "System.Button.No" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private CCombo wConnection;

  private Label wlFields;
  private TableView wFields;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  private JobEntrySQL jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  public JobEntrySQLDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntrySQL) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobSQL.Name.Default" ) );
    }
  }

  @Override
  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobSQL.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, null );

    // JobEntry Name line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobSQL.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // Connection line
    wConnection = addConnectionLine( shell, wName, middle, margin );
    if ( jobEntry.getDatabase() == null && jobMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobEntrySQLDialog.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wConnection, margin );
    wlFields.setLayoutData( fdlFields );

    final int fieldsRows = jobEntry.getSqlScripts() != null ? jobEntry.getSqlScripts().size() : 1;

    ColumnInfo[] colinf = new ColumnInfo[] {
      new ColumnInfo(
        BaseMessages.getString( PKG, "JobEntrySQLDialog.SqlScript.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo(
        BaseMessages.getString( PKG, "JobEntrySQLDialog.Comment.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo(
        BaseMessages.getString( PKG, "JobEntrySQLDialog.ReadFromFile.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        yesNoOptions ),
      new ColumnInfo(
        BaseMessages.getString( PKG, "JobEntrySQLDialog.Variables.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        yesNoOptions ), };

    wFields =
      new TableView(
        jobEntry, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    // Add listeners
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSQLDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( jobEntry.getName() ) );
    DatabaseMeta dbinfo = jobEntry.getDatabase();
    if ( dbinfo != null && dbinfo.getName() != null ) {
      wConnection.setText( dbinfo.getName() );
    } else {
      wConnection.setText( "" );
    }

    List<JobEntrySQL.SQLScript> scripts = jobEntry.getSqlScripts();
    if ( scripts != null && scripts.size() > 0 ) {
      for ( int i = 0; i < scripts.size(); i++ ) {
        TableItem row = wFields.table.getItem( i );
        row.setText( 1, scripts.get( i ).getSqlScript() );
        row.setText( 2, scripts.get( i ).getComments() );
        if ( scripts.get( i ).isReadFromFile() ) {
          row.setText( 3, yesNoOptions[0] );
        } else {
          row.setText( 3, yesNoOptions[1] );
        }
        if ( scripts.get( i ).isUseVariableSubstitution() ) {
          row.setText( 4, yesNoOptions[0] );
        } else {
          row.setText( 4, yesNoOptions[1] );
        }
      }
    }

    wFields.setRowNums();
    wFields.optWidth( true );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setDatabase( jobMeta.findDatabase( wConnection.getText() ) );

    int nrScripts = wFields.nrNonEmpty();
    List<JobEntrySQL.SQLScript> scripts = new ArrayList<JobEntrySQL.SQLScript>();
    for ( int i = 0; i < nrScripts; i++ ) {
      TableItem row = wFields.getNonEmpty( i );
      String sql = row.getText( 1 );
      String comment = row.getText( 2 );
      boolean readFromFile = BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( row.getText( 3 ) );
      boolean varSubstitute = BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( row.getText( 4 ) );
      JobEntrySQL.SQLScript script = new JobEntrySQL.SQLScript();
      script.setSqlScript( sql );
      script.setComments( comment );
      script.setReadFromFile( readFromFile );
      script.setUseVariableSubstitution( varSubstitute );
      scripts.add( script );
    }
    jobEntry.setSqlScripts( scripts );
    dispose();
  }
}
