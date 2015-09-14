/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.sftpput;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.pentaho.di.TestUtilities;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.job.entries.sftp.SftpServer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransTestFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Khayrutdinov
 */
public class SFTPPutIntegrationTest {

  private static TemporaryFolder folder;

  private static SftpServer server;

  @BeforeClass
  public static void startServer() throws Exception {
    KettleEnvironment.init();

    folder = new TemporaryFolder();
    folder.create();

    server = SftpServer.createDefaultServer( folder );
    server.start();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    server.stop();
    server = null;

    folder.delete();
    folder = null;
  }


  private Session session;
  private ChannelSftp channel;

  @Before
  public void setUp() throws Exception {
    session = server.createJschSession();
    session.connect();

    channel = (ChannelSftp) session.openChannel( "sftp" );
  }

  @After
  public void tearDown() throws Exception {
    if ( channel != null && channel.isConnected() ) {
      channel.disconnect();
    }
    channel = null;

    if ( session.isConnected() ) {
      session.disconnect();
    }
    session = null;
  }


  /**
   * This case relates to <a href="http://jira.pentaho.com/browse/PDI-13897">PDI-13897</a>.
   * It executes a transformation with two steps: data grid and sftp put.
   * The latter uploads to an SFTP server a file <tt>pdi-13897/uploaded.txt</tt>, that contains a
   * <tt>qwerty</tt> string.<br/>
   *
   * Parameters of the transformation are:
   * <ul>
   *  <li>server</li>
   *  <li>port</li>
   *  <li>username</li>
   *  <li>password</li>
   * </ul>
   * @throws Exception
   */
  @Test
  public void putFileStreamingContentFromField() throws Exception {
    // prepare a directory for transformation's execution
    channel.connect();
    channel.mkdir( "pdi-13897" );

    // execute the transformation
    Trans trans = TestUtilities.loadAndRunTransformation(
      "testfiles/org/pentaho/di/trans/steps/sftpput/pdi-13897.ktr",
      "server", "localhost",
      "port", server.getPort(),
      "username", server.getUsername(),
      "password", server.getPassword()
    );
    assertEquals( 0, trans.getErrors() );

    // verify the results
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    channel.cd( "pdi-13897" );
    channel.get( "uploaded.txt", os );
    String content = new String( os.toByteArray() );
    assertEquals( "qwerty", content );
  }

  @Test
  public void putFileFromStream() throws Exception {

    // Build a transformation
    String stepName = "SFTP Put Test";
    String inputFieldname = "My Binary Data";
    String remoteFolderFieldname = "remote_folder";
    String remoteFileFieldname = "remote_filename";
    SFTPPutMeta stepMeta = new SFTPPutMeta();
    stepMeta.setDefault();
    stepMeta.setServerName( "localhost" );
    stepMeta.setServerPort( String.valueOf( server.getPort() ) );
    stepMeta.setUserName( server.getUsername() );
    stepMeta.setPassword( server.getPassword() );
    stepMeta.setInputStream( true );
    stepMeta.setSourceFileFieldName( inputFieldname );
    stepMeta.setCreateRemoteFolder( true );
    stepMeta.setRemoteDirectoryFieldName( remoteFolderFieldname );
    stepMeta.setRemoteFilenameFieldName( remoteFileFieldname );

    TransMeta transMeta = TransTestFactory.generateTestTransformation( null, stepMeta, stepName );
    
    // Generate test data
    byte[] rawData = new byte[20];
    new Random().nextBytes( rawData );

    List<RowMetaAndData> inputData = new ArrayList<RowMetaAndData>();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( remoteFolderFieldname ) );
    rowMeta.addValueMeta( new ValueMetaString( remoteFileFieldname ) );
    rowMeta.addValueMeta( new ValueMetaBinary( inputFieldname ) );
    inputData.add( new RowMetaAndData( rowMeta, new Object[]{ "mydir", "result.bin", rawData } ) );

    List<RowMetaAndData> resultRows =
      TransTestFactory.executeTestTransformation( transMeta, TransTestFactory.INJECTOR_STEPNAME, stepName,
        TransTestFactory.DUMMY_STEPNAME, inputData );

    assertEquals( inputData, resultRows );

    // Check File Results on Server
    channel.connect();
    channel.cd( "mydir" );
    boolean foundRemoteFile = false;
    for ( Object entry : channel.ls(  "." ) ) {
      if ( entry instanceof ChannelSftp.LsEntry ) {
        if ( ( (ChannelSftp.LsEntry) entry ).getFilename().equals( "result.bin" ) ) {
          foundRemoteFile = true;
        }
      }
    }
    assertTrue( foundRemoteFile );
    byte[] resultData = IOUtils.toByteArray( channel.get( "result.bin" ) );
    assertArrayEquals( rawData, resultData );
  }
}
