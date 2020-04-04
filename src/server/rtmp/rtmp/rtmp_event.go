package rtmp

// http://help.adobe.com/zh_CN/AIR/1.5/jslr/flash/events/NetStatusEvent.html

const (
	Response_OnStatus = "onStatus"
	Response_Result   = "_result"
	Response_Error    = "_error"

	/* Level */
	Level_Status  = "status"
	Level_Error   = "error"
	Level_Warning = "warning"

	/* Code */
	/* NetStream */
	NetStream_Play_Reset          = "NetStream.Play.Reset"          // "status" 
	NetStream_Play_Start          = "NetStream.Play.Start"          // "status" 
	NetStream_Play_StreamNotFound = "NetStream.Play.StreamNotFound" // "error" 
	NetStream_Play_Stop           = "NetStream.Play.Stop"           // "status" 
	NetStream_Play_Failed         = "NetStream.Play.Failed"         // "error"  

	NetStream_Play_Switch   = "NetStream.Play.Switch"
	NetStream_Play_Complete = "NetStream.Play.Switch"

	NetStream_Data_Start = "NetStream.Data.Start"

	NetStream_Publish_Start     = "NetStream.Publish.Start"     // "status"
	NetStream_Publish_BadName   = "NetStream.Publish.BadName"   // "error"
	NetStream_Publish_Idle      = "NetStream.Publish.Idle"      // "status"
	NetStream_Unpublish_Success = "NetStream.Unpublish.Success" // "status"

	NetStream_Buffer_Empty   = "NetStream.Buffer.Empty"   // "status"
	NetStream_Buffer_Full    = "NetStream.Buffer.Full"    // "status"
	NetStream_Buffe_Flush    = "NetStream.Buffer.Flush"   // "status"
	NetStream_Pause_Notify   = "NetStream.Pause.Notify"   // "status"
	NetStream_Unpause_Notify = "NetStream.Unpause.Notify" // "status"

	NetStream_Record_Start    = "NetStream.Record.Start"    // "status"	
	NetStream_Record_NoAccess = "NetStream.Record.NoAccess" // "error"
	NetStream_Record_Stop     = "NetStream.Record.Stop"     // "status"	
	NetStream_Record_Failed   = "NetStream.Record.Failed"   // "error"	

	NetStream_Seek_Failed      = "NetStream.Seek.Failed"      // "error"
	NetStream_Seek_InvalidTime = "NetStream.Seek.InvalidTime" // "error"	
	NetStream_Seek_Notify      = "NetStream.Seek.Notify"      // "status"	

	/* NetConnect */
	NetConnection_Call_BadVersion     = "NetConnection.Call.BadVersion"     // "error"
	NetConnection_Call_Failed         = "NetConnection.Call.Failed"         // "error"	NetConnection.call .
	NetConnection_Call_Prohibited     = "NetConnection.Call.Prohibited"     // "error"	Action Message Format (AMF)
	NetConnection_Connect_AppShutdown = "NetConnection.Connect.AppShutdown" // "error"	
	NetConnection_Connect_InvalidApp  = "NetConnection.Connect.InvalidApp"  // "error"	
	NetConnection_Connect_Success     = "NetConnection.Connect.Success"     // "status"	
	NetConnection_Connect_Closed      = "NetConnection.Connect.Closed"      // "status"	
	NetConnection_Connect_Failed      = "NetConnection.Connect.Failed"      // "error"	
	NetConnection_Connect_Rejected    = "NetConnection.Connect.Rejected"    // "error"  

	/* SharedObject */
	SharedObject_Flush_Success  = "SharedObject.Flush.Success"  //"status"
	SharedObject_Flush_Failed   = "SharedObject.Flush.Failed"   //"error"	
	SharedObject_BadPersistence = "SharedObject.BadPersistence" //"error"
	SharedObject_UriMismatch    = "SharedObject.UriMismatch"    //"error"
)

type NetStatusEvent struct {
	Code  string
	Level string
}

func newNetStatusEvent(code, level string) (e *NetStatusEvent) {
	e.Code = code
	e.Level = level
	return e
}
