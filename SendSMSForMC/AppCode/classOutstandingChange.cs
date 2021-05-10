using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;
using System.Data;
using SendSMSForMC.AppCode;
using System.Globalization;

namespace SendSMSForMC
{
    class classOutstandingChange
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();
        //private static List<string[]> _currencyMapping = new List<string[]>();
        //private static List<string[]> _specialCardList = new List<string[]>();
        //private static string DEFAULT_CRNCY_ALPA = "MTT";
        public static string SMS_TYPE = "TXNMSG";

        private static string SCBPhone = "";
        
        public static void RunService()
        {
           try
           {
                int minute = 0;
                int hour = 0;               
                int value = 0;
                DataTable table = new DataTable();
                //_currencyMapping = classUtilities.ReadMappingFile("currency_mapping.txt");
                //_specialCardList = classUtilities.ReadMappingFile("special_card_list.txt");

                //SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
                value = classUtilities.GetIntValueFromConfig("Outstanding_Change_Minute");
                int sleep = classUtilities.GetIntValueFromConfig("time_sleep");
                while (_exitThread == false)
                {
                    minute = DateTime.Now.Minute;
                    hour = DateTime.Now.Hour; 
                   
                    if(minute % value == 0)
                    {
                        
                        classOutstandingChangeLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();                     
                        table.Rows.Clear();
                        classOutstandingChangeLogWriter.WriteLog("begin Get_Outstanding_Change()");
                        table = Get_Outstanding_Change();
                        classOutstandingChangeLogWriter.WriteLog("end Get_Outstanding_Change()");
                        if (table.Rows.Count > 0)
                        {
                            classOutstandingChangeLogWriter.WriteLog("begin Insert_SMSMessage()");
                            Insert_SMSMessage(table);
                            classOutstandingChangeLogWriter.WriteLog("end Insert_SMSMessage()");
                        }
                        classOutstandingChangeLogWriter.WriteLog("----------------End Process-----------------");
                        if (value > 2)
                            Thread.Sleep(1000 * (value - 1) * 55);// sleep (value - 1) phut de troi qua thoi gian lap lai
                        
                    }
                    classOutstandingChangeLogWriter.WriteLog("sleep (s)" + sleep);
                    Thread.Sleep(1000 * sleep);
                }
        }
        catch (Exception ex)
        {
            classOutstandingChangeLogWriter.WriteLog("Error RunService(), " + ex.Message);
        }
    }
        private static void SendSMSForPhone_new(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit, string mess, string phone)
        {
            try
            {
               
                string status = (creditOrDebit == "Giao dich:" ? " " : "C");//neu la giao dich reversal thi status = "C"
               
                
                int flag_fpt = dwDataAccess.InsertOutBalChangeSMSToDW(SMS_TYPE, mess
                        , phone
                        , DateTime.Parse(row.ItemArray[0].ToString())                    
                        , row.ItemArray[17].ToString()
                        , row.ItemArray[2].ToString(), row.ItemArray[3].ToString()
                        , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                        , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                        , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                        , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                        , row.ItemArray[9].ToString()
                        , row.ItemArray[11].ToString()
                        , row.ItemArray[12].ToString()
                        , status
                        , row.ItemArray[1].ToString()
                        , "T"//Test so du thuc te

                        , row.ItemArray[24].ToString()
                        , row.ItemArray[25].ToString()
                        , row.ItemArray[26].ToString()
                        , row.ItemArray[27].ToString()
                        , row.ItemArray[23].ToString()
                        , row.ItemArray[19].ToString()
                        , row.ItemArray[18].ToString()
                        );
                if (flag_fpt == 0)
                {

                    classOutstandingChangeLogWriter.WriteLog("err: card no " + row.ItemArray[17].ToString() + " can't insert DB DW");
                   

                }
              
               
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendSMSForPhone_new():" + ex.Message);
                
            }
        }
       
        private static int SendSMSForPhone(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit,string mess, string phone)
        {
            try
            {
                   
                    int flag_return = 0;
                    int result = 0;                   
                    if (string.IsNullOrEmpty(mess.Trim()) == false)
                    {
                        string status = (creditOrDebit == "Giao dich:" ? " " : "C");//neu la giao dich reversal thi status = "C"
                        if (phone == "khong co")// so phone ko hop le
                        {
                                int flag_fpt= dwDataAccess.InsertOutBalChangeSMSToDW(SMS_TYPE, mess
                                        , phone
                                        , DateTime.Parse(row.ItemArray[0].ToString())
                                        , row.ItemArray[17].ToString()
                                        , row.ItemArray[2].ToString(), row.ItemArray[3].ToString()
                                        , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                        , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                        , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                        , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                        , row.ItemArray[9].ToString()
                                        , row.ItemArray[11].ToString()
                                        , row.ItemArray[12].ToString()
                                        , status
                                        , row.ItemArray[1].ToString()
                                        , "Y"//khong co so dt, tin nhan ko duoc gui di
                                        , row.ItemArray[24].ToString()
                                        , row.ItemArray[25].ToString()
                                        , row.ItemArray[26].ToString()
                                        , row.ItemArray[27].ToString()
                                        , row.ItemArray[23].ToString()
                                        , row.ItemArray[19].ToString()
                                        , row.ItemArray[18].ToString()
                                        );
                                if( flag_fpt==0)
                                {
                                   
                                    
                                    classOutstandingChangeLogWriter.WriteLog("err: card no " + row.ItemArray[17].ToString() + " can't insert DB DW");
                                    flag_return = 1; //intert DW khong thanh cong

                                }
                                else//inster DW thanh cong
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW(
                                        //classDataAccess.IDALERT
                                                                       row.ItemArray[16].ToString()
                                                                       , phone
                                                                       , mess
                                                                       , 'Y'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                                       , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        classOutstandingChangeLogWriter.WriteLog("err: card no " + row.ItemArray[17].ToString() + " can't insert Ebanking GW");
                                        flag_return = 2; //2: inster EB khong thanh cong

                                    }
                                    else
                                        flag_return = 3;//3: insert EB thanh cong

                                }
                          
                        }
                        else //so phone hop le
                        {   
                                int flag_fpt = dwDataAccess.InsertOutBalChangeSMSToDW(SMS_TYPE, mess
                                , phone
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                , row.ItemArray[17].ToString()
                                , row.ItemArray[2].ToString(), row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[11].ToString()
                                , row.ItemArray[12].ToString()
                                , status
                                , row.ItemArray[1].ToString()
                                , "N"//normal: insert DW thanh cong
                                , row.ItemArray[24].ToString()
                                , row.ItemArray[25].ToString()
                                , row.ItemArray[26].ToString()
                                , row.ItemArray[27].ToString()
                                , row.ItemArray[23].ToString()
                                , row.ItemArray[19].ToString()
                                , row.ItemArray[18].ToString()
                                );                                
                                if (flag_fpt == 0)
                                {
                                    classOutstandingChangeLogWriter.WriteLog("err: card no " + row.ItemArray[17].ToString() + " can't insert DB DW");
                                    flag_return = 1; //intert DW khong thanh cong
                                }
                                else//inster DW thanh cong
                                {
                                    classOutstandingChangeLogWriter.WriteLog("done for InsertOutBalChangeSMSToDW");
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW(
                                        //classDataAccess.IDALERT
                                                                       row.ItemArray[16].ToString()
                                                                       , phone
                                                                       , mess
                                                                       , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                                       , SMS_TYPE);
                                    if (result == 0)
                                    {

                                        int flag = dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                        row.ItemArray[17].ToString(),
                                        row.ItemArray[5].ToString(),
                                        row.ItemArray[11].ToString(),
                                        "E"
                                        );
                                        classOutstandingChangeLogWriter.WriteLog("err: card no " + row.ItemArray[17].ToString() + " can't insert Ebanking GW");
                                        flag_return = 2; //2: inster EB khong thanh cong

                                    }
                                    else
                                    {
                                        classOutstandingChangeLogWriter.WriteLog("done for InsertSMSMessateToEBankGW");
                                        flag_return = 3; //3: insert EB thanh cong
                                    }

                                }
                           
                        }
                       
                    }
                    return flag_return;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendSMSForPhone():" + ex.Message);
                return 0;//0: loi
            }
        }
        private static void SendSMSForLossMapCard(string mobile, int loss)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check table IM.ir_pan_map," +loss +" card loss on this table.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendSMSForLossMapCard():" + ex.Message);
            }
        }

        private static void SendSMSForDW_FPT(string mobile, int err)
        {
            try
            {
                
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check table FPT.smsmastercard, " + err + " SMS can't insert DB DW for Outstanding change.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendSMSForDW_FPT():" + ex.Message);
            }
        }
        private static void SendSMSForGW_EB(string mobile, int err)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check connect to Ebanking GW," + err +" SMS can't insert data for Outstanding change.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendSMSForGW_EB():" + ex.Message);
            }
        }
        private static int SendOneSMS(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit, int flag_reversal)
        {
            try
            {
                int flag_return = -1;
                string card_tmp = "";
                string available = "";
                if (double.Parse(row.ItemArray[18].ToString()) < double.Parse(row.ItemArray[19].ToString()))
                    available = row.ItemArray[18].ToString();
                else
                    available = row.ItemArray[19].ToString();

                if (row.ItemArray[4].ToString() == "")//the bi thieu bang map card (ir_pan_map)
                {

                    card_tmp = _dataAccess.GetCardNo(row.ItemArray[17].ToString());
                    classOutstandingChangeLogWriter.WriteLog("errr: card no " + row.ItemArray[17].ToString() + " is loss on ir_pan_map");
                    flag_return = 4;//thieu bang map card

                }
                else
                {
                    card_tmp = row.ItemArray[4].ToString();
                }
                
                string message = null;
                             
                {
                    if (creditOrDebit == "Giao dich:")//gd thuan
                    {
                       
                            //message = CreateSMSCreditMessageSplitLimit(row.ItemArray[2].ToString(), card_tmp,// row.ItemArray[4].ToString(), 
                            //                           row.ItemArray[5].ToString(),
                            //                           row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                            //                           row.ItemArray[8].ToString(), row.ItemArray[9].ToString(), //row.ItemArray[18].ToString(),
                            //                           available,
                            //                           flag_reversal,
                            //                           row.ItemArray[20].ToString(), row.ItemArray[21].ToString(), row.ItemArray[22].ToString());//neu flag_reversal=1 la giao dich reversal nhung chua gui giao dich thuan
                            message = CreateSMSCreditMessageSplitLimit_160(row.ItemArray[2].ToString(), card_tmp,// row.ItemArray[4].ToString(), 
                                                       row.ItemArray[5].ToString(),
                                                       row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                       row.ItemArray[8].ToString(), row.ItemArray[9].ToString(), //row.ItemArray[18].ToString(),
                                                       available,
                                                       flag_reversal,
                                                       row.ItemArray[20].ToString(), row.ItemArray[21].ToString(), row.ItemArray[22].ToString());//neu flag_reversal=1 la giao dich reversal nhung chua gui giao dich thuan



                    }
                    else
                    {
                        //message = CreateSMSDebitMessageSplitLimit(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                        //                                    row.ItemArray[5].ToString(),
                        //                                    row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                        //                                    row.ItemArray[8].ToString(), row.ItemArray[9].ToString(), //row.ItemArray[18].ToString()
                        //                                    available, row.ItemArray[20].ToString(), row.ItemArray[22].ToString()
                        //                                    );
                        message = CreateSMSDebitMessageSplitLimit_160(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                                                           row.ItemArray[5].ToString(),
                                                           row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                           row.ItemArray[8].ToString(), row.ItemArray[9].ToString(), //row.ItemArray[18].ToString()
                                                           available, row.ItemArray[20].ToString(), row.ItemArray[22].ToString()
                                                           );
                        
                    }
                }
                
                if (row.ItemArray[14].ToString().Trim() == "")//la the chinh (crn cua the phu)
                { 
                    flag_return=SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString());
                   
                    ///////////over limit                
                    if (double.Parse(row.ItemArray[18].ToString()) < 0)//han muc con lai < 0
                    {
                        string mess_overlimit = CreateSMSOverLimit(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                                                                row.ItemArray[5].ToString(),
                                                                row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                                row.ItemArray[8].ToString(), row.ItemArray[9].ToString(), row.ItemArray[18].ToString()
                                                                , row.ItemArray[20].ToString(), row.ItemArray[22].ToString());
                        flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, mess_overlimit, row.ItemArray[10].ToString());
                    }
                                            
                }
                else//giao dich cua the phu
                {
                    
                    //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList2(row.ItemArray[17].ToString());                
                    //if (mobile == "000")// the phu khong thuoc ds dac biet

                    if (row.ItemArray[29].ToString().Trim() == "1")//la the quan ly
                    {
                        flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString());
                    }
                    else// the phu khong phai la the quan ly
                    {
                        if (row.ItemArray[30].ToString().Trim() != "E")//the phu co the quan ly (man_phone <> E)
                        {
                            flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString());
                            if (row.ItemArray[28].ToString().Trim() == "1")//1:gui sms cho the chinh or the quan ly
                                flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[30].ToString());
                        }
                        else //the phu co the chinh
                        {
                            if (row.ItemArray[28].ToString() == "1")// the phu co gui sms cho the chinh
                            {
                                flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString());
                                flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString());


                            }
                            else // the co field sms inf=N or the phu thuoc ds dac biet
                            {
                                flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString());


                            }
                        }
                    }
                   
                    ///////////over limit: neu the phu chi gui sms cho the phu               
                    if (double.Parse(row.ItemArray[18].ToString()) < 0)//han muc con lai < 0
                    {
                        string mess_overlimit = CreateSMSOverLimit(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                                                                row.ItemArray[5].ToString(),
                                                                row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                                row.ItemArray[8].ToString(), row.ItemArray[9].ToString(),
                                                                row.ItemArray[18].ToString(), row.ItemArray[20].ToString(), row.ItemArray[22].ToString());
                        flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, mess_overlimit, row.ItemArray[15].ToString());
                    }
                }

                //sms them cho the co 2 phone
                //////////////
                foreach (string[] item in classUtilities._2PhoneCardList)
                {

                    if (item[0] == row.ItemArray[17].ToString())
                    {
                        flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, item[2]);
                    }

                }
                ////////////
                
                return flag_return;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error SendOneSMS():" + ex.Message);
                return 0;
            }
        }

        private static void Insert_SMSMessage(DataTable table)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                classDataAccess dwDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");
                dwDataAccess.OpenConnection("CW_DW");
                string mobile = classUtilities.GetStringValueFromConfig("MyPhone1");
                string mobile2 = classUtilities.GetStringValueFromConfig("MyPhone2");
                string mobile3 = classUtilities.GetStringValueFromConfig("MyPhone3");
                //string mobile4 = classUtilities.GetStringValueFromConfig("MyPhone4");
                string mobile7 = classUtilities.GetStringValueFromConfig("MyPhone7");               
                int countSend =0;
                int countReversalSend = 0;
                int flag_sendSMS = -1;
                int flag_sendSMSRe = -1;
                int err_DW = 0;
                int err_EB = 0;
                int err_mapcard = 0;
           
                foreach (DataRow row in table.Rows)
                {

                    if (string.IsNullOrEmpty(row.ItemArray[13].ToString().Trim()) == false)// la giao dich reversal(status <> "")
                    {
                        if (IsExistedSMSOnDW(//row.ItemArray[4].ToString()
                                               row.ItemArray[17].ToString()
                            , row.ItemArray[7].ToString()
                                            , row.ItemArray[11].ToString(), row.ItemArray[5].ToString()) == false)// chua gui tin nhan giao dich thuan
                        {
                            //1: gd reversal nhung chua gui sms thuan
                            flag_sendSMS=SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:",1);

                           
                            if(flag_sendSMS==3)//insert EB thanh cong
                                countSend++;
                        }
                        

                        flag_sendSMSRe = SendOneSMS(ebankDataAccess, dwDataAccess, row, "Hoan tra:",0); 
                        
                        if (flag_sendSMS == 3)//insert EB thanh cong
                            countReversalSend++;
                    }
                    else// la giao dich thuan
                    {
                        
                        flag_sendSMS=SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:",0);
                       
                        if (flag_sendSMS == 3)//insert EB thanh cong
                            countSend++;
                    }
                    if (flag_sendSMS == 1 || flag_sendSMSRe == 1)
                        err_DW++;
                    if (flag_sendSMS == 2 || flag_sendSMSRe == 2)
                        err_EB++;
                    if (flag_sendSMS == 4 || flag_sendSMSRe == 4)
                        err_mapcard++;
                }
                if(err_DW > 0)
                {
                    SendSMSForDW_FPT(mobile, err_DW);
                    SendSMSForDW_FPT(mobile2, err_DW);
                    SendSMSForDW_FPT(mobile3, err_DW);
                    //SendSMSForDW_FPT(mobile4, err_DW);
                    SendSMSForDW_FPT(mobile7, err_DW);
                }
                if(err_EB >0)
                {
                    SendSMSForGW_EB(mobile, err_EB);
                    SendSMSForGW_EB(mobile2, err_EB);
                    SendSMSForGW_EB(mobile3, err_EB);
                    //SendSMSForGW_EB(mobile4, err_EB);
                    SendSMSForGW_EB(mobile7, err_EB);
                }
                if(err_mapcard > 0)
                {
                    SendSMSForLossMapCard(mobile, err_mapcard);
                    SendSMSForLossMapCard(mobile2, err_mapcard);
                    SendSMSForLossMapCard(mobile3, err_mapcard);
                    //SendSMSForLossMapCard(mobile4, err_mapcard);
                    SendSMSForLossMapCard(mobile7, err_mapcard);
                }

                ebankDataAccess.CloseConnection();
                dwDataAccess.CloseConnection();
                classOutstandingChangeLogWriter.WriteLog("Message giao dich thuan da duoc Insert vao EbankGW thanh cong: " + countSend);
                classOutstandingChangeLogWriter.WriteLog("Message giao dich reversal da duoc Insert vao EbankGW thanh cong: " + countReversalSend);
                return;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error Insert_SMSMessage() " + ex.Message);
            }
           
        }

        private static bool IsExistedSMSOnDW(string pan, string amount, string apvCode, string transDate)
        {
            try
            {
                DataTable table = _dataAccess.Find_OutBal_SMS(pan, double.Parse(amount), apvCode, transDate);
                if (table.Rows.Count > 0)// da ton tai giao dich thuan
                    return true;
                return false;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error IsExistedSMSOnDW() " + ex.Message);
                return false;
            }
        }

        private static DataTable Get_Outstanding_Change()
        {
            DataTable table = new DataTable();
            try
            {
                string maxUpdateDT = null;                
                table.Rows.Clear();
                if (string.IsNullOrEmpty(_updateDateTime) == false)
                {
                    maxUpdateDT = _updateDateTime;
                    _updateDateTime = null;
                }
                else
                {
                    maxUpdateDT = _dataAccess.Get_Max_UpdateTime_Out(SMS_TYPE);
                    //maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);//hoannd change 20/04/2015
                    /*
                    DataTable updateTime = _dataAccess.Get_Max_OutBal_UpdateTime(SMS_TYPE);
                    if (updateTime.Rows.Count == 1 || updateTime.Rows.Count >= 2)//hoannd change 17/04/2015
                        maxUpdateDT = updateTime.Rows[0].ItemArray[0].ToString();
                     */

                }
                //if (string.IsNullOrEmpty(maxUpdateDT) == false)
                //{

                //    long maxUpdateTime = long.Parse(maxUpdateDT);
                //}//hoand rem 02102015
                if (maxUpdateDT != null)//hoannd add 17/04/2015
                    table = _dataAccess.GetOutstandingChange(maxUpdateDT);
                    return table;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error Get_Outstanding_Change() 1, " + ex.Message);
                table.Rows.Clear();
                return table;
            }
           
        }
        //private static string CreateSMSDebitMessage(string brand, string pan, string date, string time,
        //                               string amount, string crncyCode, string merchaneName)
        //{
        //    try
        //    {
        //        pan = pan.Substring(12, 4);
        //        date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
        //        time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
        //        time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
        //        string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
        //        double amt = double.Parse(amount);
        //        string amount1 = string.Format("{0:#,##0.##}", amt);
        //        string smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the " + brand + " x" + pan
        //        + " do GD khong thanh cong tai " + merchaneName + " luc " + time + " " + date + "\nDe biet them chi tiet LH 1800545438.";
        //        return smsMessage;
        //    }
        //    catch (Exception ex)
        //    {
        //        classOutstandingChangeLogWriter.WriteLog("Error CreateSMSDebitMessage(), " + ex.Message);
        //        return "";
        //    }
        //}
        //private static string CreateSMSCreditMessage(string brand, string pan, string date, string time,
        //                                string amount, string crncyCode, string merchaneName)
        //{
        //    try
        //    {
        //        pan = pan.Substring(12, 4);
        //        date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
        //        time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
        //        time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
        //        string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
        //        double amt = double.Parse(amount);
        //        string amount1 = string.Format("{0:#,##0.##}", amt);
        //        string smsMessage = "Cam on Quy khach da su dung the SCB " + brand + " x" + pan + " giao dich thanh cong " + amount1 + crncyAlpha
        //        + "\nLuc " + time + " " + date + "\nTai " + merchaneName + "\nChi tiet LH: 1800545438";               

        //        return smsMessage;
        //    }
        //    catch (Exception ex)
        //    {
        //        classOutstandingChangeLogWriter.WriteLog("Error CreateSMSCreditMessage(), " + ex.Message);
        //        return "";
        //    }
        //}
        
        private static string CreateSMSDebitMessageSplitLimit(string brand, string pan, string date, string time,
                                      string amount, string crncyCode, string merchaneName, string Available_bal,string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                double avai_bal = double.Parse(Available_bal);
                string str_avai_bal = string.Format("{0:#,##0.##}", avai_bal);
                string smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the " + brand + " x" + pan
                + " do GD khong thanh cong tai " + merchaneName + " luc " + time + " " + date + ". HM con lai " + str_avai_bal + "VND. De biet them chi tiet LH " + SCBPhone;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSDebitMessageSplitLimit(), " + ex.Message);
                return "";
            }
        }
        //hhhh
        private static string CreateSMSDebitMessageSplitLimit_160(string brand, string pan, string date, string time,
                                     string amount, string crncyCode, string merchaneName, string Available_bal, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                double avai_bal = double.Parse(Available_bal);
                string str_avai_bal = string.Format("{0:#,##0.##}", avai_bal);
                string smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the x" + pan
                + "\nGD khong thanh cong tai " + merchaneName + " luc " + time + " " + date + "\nHM con lai " + str_avai_bal + "VND.\nLH " + SCBPhone;
                if (smsMessage.Length > 160)
                {
                    if (merchaneName.Length > (smsMessage.Length - 160))
                    {
                        string new_marchant = merchaneName.Substring(0, merchaneName.Length - (smsMessage.Length - 160));
                        smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the x" + pan
                        + "\nGD khong thanh cong tai " + new_marchant + " luc " + time + " " + date + "\nHM con lai " + str_avai_bal + "VND.\nLH " + SCBPhone;
                    }
                }
                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSDebitMessageSplitLimit_160(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSCreditMessageSplitLimit_160(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchaneName, string Available_bal, int flag_reversal, string vip_card, string amt_req, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                double avai_bal = 0;
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                if (flag_reversal == 1)//gd reversal nhung chua gui sms thuan
                    avai_bal = double.Parse(Available_bal) - double.Parse(amt_req);
                else
                    avai_bal = double.Parse(Available_bal);

                string str_avai_bal = string.Format("{0:#,##0.##}", avai_bal);
                string smsMessage = "Cam on Quy khach da su dung the SCB X" + pan + "\nGD thanh cong " + amount1 + crncyAlpha
                + "\nLuc " + time + " " + date + "\nTai " + merchaneName + "\nHM con lai " + str_avai_bal + "VND";
                
                if (smsMessage.Length > 160)
                {
                    if (merchaneName.Length > (smsMessage.Length - 160))
                    {
                        string new_marchant = merchaneName.Substring(0, merchaneName.Length - (smsMessage.Length - 160));
                        smsMessage = "Cam on Quy khach da su dung the SCB X" + pan + "\nGD thanh cong " + amount1 + crncyAlpha
                        + "\nLuc " + time + " " + date + "\nTai " + new_marchant + "\nHM con lai " + str_avai_bal + "VND";
                    }
                }
                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSCreditMessageSplitLimit_160(), " + ex.Message);
                return "";
            }
        }

        private static string CreateSMSCreditMessageSplitLimit(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchaneName, string Available_bal, int flag_reversal, string vip_card, string amt_req,string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                double avai_bal =0;
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                if(flag_reversal==1)//gd reversal nhung chua gui sms thuan
                    avai_bal = double.Parse(Available_bal) - double.Parse(amt_req);
                else
                    avai_bal = double.Parse(Available_bal);

                string str_avai_bal = string.Format("{0:#,##0.##}", avai_bal);
                string smsMessage = "Cam on Quy khach da su dung the SCB " + brand + " x" + pan + " giao dich thanh cong " + amount1 + crncyAlpha
                + ". Luc " + time + " " + date + ". Tai " + merchaneName + ". HM con lai " + str_avai_bal + "VND. Chi tiet LH: " + SCBPhone;

                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSCreditMessageSplitLimit(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSOverLimit(string brand, string pan, string date, string time,
                                      string amount, string crncyCode, string merchaneName, string Available_bal, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                //time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                //time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                //string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                //double amt = double.Parse(amount);
                //string amount1 = string.Format("{0:#,##0.##}", amt);
                double avai_bal = double.Parse(Available_bal);
                string str_avai_bal = string.Format("{0:#,##0.##}", avai_bal);
                string smsMessage = " The SCB " + brand + " x" + pan + " vua giao dich vuot han muc " + str_avai_bal
                + "VND\nVui long thanh toan so tien vuot de tranh phat sinh phi vuot han muc\nLH " + SCBPhone;

                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSOverLimit(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage(string brand, string pan, string date, string time,
                                        string amount, string crncyCode, string merchaneName, string creditOrDebit, string vip)
        { 
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6,2) + "/" + date.Substring(4,2) + "/" + date.Substring(2,2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0,2) + ":" + time.Substring(2,2);
                if (vip == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string tmp_smsMessage = "The SCB " + brand + " x" + pan + "\n" + "Ngay: " + date + " " + time + "\n"
                                + creditOrDebit + " " + amount1 + crncyAlpha + "\n" + "Tai: ";  
                string smsMessagelh="Chi tiet LH: " + SCBPhone ;
                string smsMessage = tmp_smsMessage + merchaneName + "\n" + smsMessagelh;

                if (smsMessage.Length > 160)                    
                {
                    string smsMerchaneName=merchaneName.Substring(0, 160 - tmp_smsMessage.Length - 23);
                    // 23 la chieu dai cua chuoi: "Chi tiet LH: 1800545438"
                    smsMessage = tmp_smsMessage + smsMerchaneName + smsMessagelh;
                }

                return smsMessage;         
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
        
    }
}
