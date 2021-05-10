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
    class classGDMCDebit
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();
        //private static List<string[]> _currencyMapping = new List<string[]>();
        //private static List<string[]> _specialCardList = new List<string[]>();
        //private static string DEFAULT_CRNCY_ALPA = "MTT";
        public static string SMS_TYPE = "TXNMCD";

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

                SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
                value = classUtilities.GetIntValueFromConfig("GDMCDebit_Minute");
                int flag_send_sms = classUtilities.GetIntValueFromConfig("Flag_Check_Send_SMS");
                
                while (_exitThread == false)
                {
                    minute = DateTime.Now.Minute;
                    hour = DateTime.Now.Hour; 
                   
                    if(minute % value == 0)
                    {
                        
                        classGDMCDebitLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();                     
                        table.Rows.Clear();
                        //classGDMCDebitLogWriter.WriteLog("begin Get_GDMCDebit()");
                        table = Get_GDMCDebit();
                        //classGDMCDebitLogWriter.WriteLog("end Get_GDMCDebit()");
                        if (table.Rows.Count > 0)
                        {
                            //classGDMCDebitLogWriter.WriteLog("begin Insert_SMSMessage()");
                            Insert_SMSMessage(table, flag_send_sms);
                            //classGDMCDebitLogWriter.WriteLog("end Insert_SMSMessage()");
                        }
                        classGDMCDebitLogWriter.WriteLog("----------------End Process-----------------");
                        if (value > 2)
                            Thread.Sleep(1000 * (value - 1) * 55);// sleep (value - 1) phut de troi qua thoi gian lap lai
                        
                    }
                    //classGDMCDebitLogWriter.WriteLog("sleep 55s");
                    Thread.Sleep(1000 * 55);
                }
        }
        catch (Exception ex)
        {
            classGDMCDebitLogWriter.WriteLog("Error RunService(), " + ex.Message);
        }
    }
        private static int SendSMSForPhone_K(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit, string mess, string phone, string flag_sms)
        {
            try
            {
                
                if (string.IsNullOrEmpty(mess.Trim()) == false)
                {
                    string status = (creditOrDebit == "Giao dich:" ? " " : "C");//neu la giao dich reversal thi status = "C"
                   
                        int flag_fpt = dwDataAccess.InsertOutBalChangeMDSMSToDW(SMS_TYPE, mess
                                , phone
                                , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[4].ToString()
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
                                , flag_sms                                 
                                );
                        if (flag_fpt == 0)
                        {
                            classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can not insert DB DW");
                            return 1; //intert DW khong thanh cong
                        }  
                }
                return 3;//ko gui sms
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendSMSForPhone_K():" + ex.Message);
                return 3;//3: loi
            }
        }

       
        private static int SendSMSForPhone(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit,string mess, string phone, string flag_send)
        {
            try
            {
                int result = 0;                   
                if (string.IsNullOrEmpty(mess.Trim()) == false)
                {
                    string status = (creditOrDebit == "Giao dich:" ? " " : "C");//neu la giao dich reversal thi status = "C"
                    //if (flag_send == "Y")
                    //{
                        if (phone == "khong co")
                        {
                            int flag_fpt = dwDataAccess.InsertOutBalChangeMDSMSToDW(SMS_TYPE, mess
                                    , phone
                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
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
                                    );
                            if (flag_fpt == 0)
                            {
                                classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can not insert DB DW");
                                return 1; //intert DW khong thanh cong
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
                                    classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can not insert Ebanking GW");
                                    return 2; //2: inster EB khong thanh cong
                                }
                                else
                                    return 0;//thanh cong

                            }

                        }
                        else//so phone hop le
                        {
                            int flag_fpt = dwDataAccess.InsertOutBalChangeMDSMSToDW(
                            SMS_TYPE
                            , mess
                            , phone
                            , DateTime.Parse(row.ItemArray[0].ToString())                               
                            , row.ItemArray[17].ToString()
                            , row.ItemArray[2].ToString()
                            , row.ItemArray[3].ToString()
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
                            );
                            if (flag_fpt == 0)
                            {
                                classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can't insert DB DW");
                                return 1; //intert DW khong thanh cong
                            }
                            else//inster DW thanh cong
                            {
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
                                    classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can not insert Ebanking GW");
                                    return 2; //2: inster EB khong thanh cong

                                }
                                else
                                    return 0; //thanh cong

                            }

                        }
                    //}
                    //else// flag = N, khong nhan sms
                    //{
                    //    int flag_fpt = dwDataAccess.InsertOutBalChangeMDSMSToDW(SMS_TYPE, mess
                    //       , phone
                    //       , DateTime.Parse(row.ItemArray[0].ToString())
                    //        //, row.ItemArray[4].ToString()
                    //       , row.ItemArray[17].ToString()
                    //       , row.ItemArray[2].ToString(), row.ItemArray[3].ToString()
                    //       , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                    //       , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                    //       , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                    //       , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                    //       , row.ItemArray[9].ToString()
                    //       , row.ItemArray[11].ToString()
                    //       , row.ItemArray[12].ToString()
                    //       , status
                    //       , row.ItemArray[1].ToString()
                    //       , "K"//the ko dk nhan sms
                          
                    //       );
                    //    if (flag_fpt == 0)
                    //    {
                    //        dwDataAccess.InsertOutBalChangeMDSMSToDW(SMS_TYPE, mess
                    //        , phone
                    //        , DateTime.Parse(row.ItemArray[0].ToString())
                    //            //, row.ItemArray[4].ToString()
                    //        , row.ItemArray[17].ToString()
                    //        , row.ItemArray[2].ToString(), row.ItemArray[3].ToString()
                    //        , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                    //        , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                    //        , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                    //        , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                    //        , row.ItemArray[9].ToString()
                    //        , row.ItemArray[11].ToString()
                    //        , row.ItemArray[12].ToString()
                    //        , status
                    //        , row.ItemArray[1].ToString()
                    //        , "K"//insert lan 2: loi do ko insert duoc DW lan 1
                         
                    //        );
                    //        classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " can't insert DB DW");
                    //        return 1; 
                    //    }

                    //}
                   
                }
                return 3;//ko gui sms
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendSMSForPhone():" + ex.Message);
                return 3;//3: loi
            }
        }

        private static void SendSMSForLossMapCard(string mobile, int loss)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check table IM.ir_pan_map," + loss + " card loss on this table.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendSMSForLossMapCard():" + ex.Message);
            }
        }

        private static void SendSMSForDW_FPT(string mobile, int err)
        {
            try
            {
                
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check table FPT.smsmastercard, " + err + " SMS can't insert DB DW for MC Debit.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendSMSForDW_FPT():" + ex.Message);
            }
        }
        private static void SendSMSForGW_EB(string mobile, int err)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check connect to Ebanking GW," + err + " SMS can not insert data for MC Debit.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendSMSForGW_EB():" + ex.Message);
            }
        }
        private static void SendOneSMS(classDataAccess ebankDataAccess, classDataAccess dwDataAccess, DataRow row, string creditOrDebit, int flag_send_sms, int err_dw, int err_eb)
        {
            try
            {
                err_dw = 0;
                err_eb = 0;
                int flag_return_p = -1;
                int flag_return = -1;
                string card_tmp = "";              
                if (row.ItemArray[4].ToString() == "")//the bi thieu bang map card (ir_pan_map)
                {
                    card_tmp = _dataAccess.GetCardNo(row.ItemArray[17].ToString());
                    classGDMCDebitLogWriter.WriteLog("card no " + row.ItemArray[17].ToString() + " is loss on ir_pan_map" );
                    flag_return = 4;//thieu bang map card

                }
                else
                {
                    card_tmp = row.ItemArray[4].ToString();
                }
                string message=null;
                
                if (creditOrDebit == "Giao dich:")
                    message = CreateSMSCreditMessage(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                                                        row.ItemArray[5].ToString(),
                                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                        row.ItemArray[8].ToString(), row.ItemArray[9].ToString()
                                                        , row.ItemArray[19].ToString(), row.ItemArray[20].ToString());
                else
                    message = CreateSMSDebitMessage(row.ItemArray[2].ToString(), card_tmp,//row.ItemArray[4].ToString(), 
                                                        row.ItemArray[5].ToString(),
                                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                        row.ItemArray[8].ToString(), row.ItemArray[9].ToString()
                                                        , row.ItemArray[19].ToString(), row.ItemArray[20].ToString());
              if (row.ItemArray[14].ToString().Trim() == "")//giao dich cua the chinh(gia tri truong CRN the phu khong co)
                {
                    //if (flag_send_sms==0)//tat chuc nang kiem tra the co dk send sms hay khong
                    //    flag_return=SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString(), "Y");//truyen tham so mac dinh co gui sms
                    //else
                    //    flag_return=SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString(), row.ItemArray[18].ToString());
                    flag_return_p = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString(), "Y");//truyen tham so mac dinh co gui sms
                    

                }
                else//giao dich cua the phu
                {
                    //if (flag_send_sms == 0)//tat chuc nang kiem tra the co dk send sms hay khong
                    //    flag_return=SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString(), "Y");//truyen tham so mac dinh co gui sms
                    //else
                    //    flag_return=SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString(), row.ItemArray[18].ToString());
                    if (row.ItemArray[21].ToString().Trim() == "1")//la the quan ly
                    {   
                        //if(row.ItemArray[22].ToString()!=row.ItemArray[10].ToString())
                            flag_return_p = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[22].ToString(), "Y");//gui sms cho the quan ly
                        //else
                            //flag_return_p = SendSMSForPhone_K(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[22].ToString(), "K");//khong gui sms cho the quan ly vi trung so dt                        
                    }
                    else // the phu khong phai la the quan ly
                    {
                        if (row.ItemArray[22].ToString().Trim() != "E")//the phu co the quan ly (man_phone <> E)
                        {
                            flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString(), "Y");//gui sms cho the phu
                            if (row.ItemArray[23].ToString().Trim() == "1")//1:gui sms cho the chinh or the quan ly
                                flag_return_p = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[22].ToString(), "Y");//gui sms cho the quan ly
                        }
                        else //the phu co the chinh
                        {
                            flag_return = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[15].ToString(), "Y");//gui sms cho the phu
                            if (row.ItemArray[23].ToString().Trim() == "1")//1:gui sms cho the chinh or the quan ly
                                flag_return_p = SendSMSForPhone(ebankDataAccess, dwDataAccess, row, creditOrDebit, message, row.ItemArray[10].ToString(), "Y");//gui sms cho the chinh

                        }
                    }
                 
                }
              if (flag_return == 1)
                  err_dw ++;
              else
                  if (flag_return == 2)
                      err_eb ++;

              if (flag_return_p == 1)
                  err_dw ++;
              else
                  if (flag_return_p == 2)
                      err_eb ++;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error SendOneSMS():" + ex.Message);
                
            }
        }

        private static void Insert_SMSMessage(DataTable table, int flag_send_sms)
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
                int tol_err_EB=0;
                int tol_err_DW=0;
           
                foreach (DataRow row in table.Rows)
                {
                    if (string.IsNullOrEmpty(row.ItemArray[13].ToString().Trim()) == false)// la giao dich reversal(status <> "")
                    {
                        if (IsExistedSMSOnDW(//row.ItemArray[4].ToString()
                                               row.ItemArray[17].ToString()
                            , row.ItemArray[7].ToString()
                                            , row.ItemArray[11].ToString(), row.ItemArray[5].ToString()) == false)// chua gui tin nhan giao dich thuan
                        {

                            //flag_sendSMS=SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:", flag_send_sms);  
                            SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:", flag_send_sms, err_DW, err_EB);
                            countSend++;
                        }


                        //flag_sendSMSRe=SendOneSMS(ebankDataAccess, dwDataAccess, row, "Hoan tra:", flag_send_sms); 
                        SendOneSMS(ebankDataAccess, dwDataAccess, row, "Hoan tra:", flag_send_sms, err_DW, err_EB); 

                        
                        countReversalSend++;
                    }
                    else// la giao dich thuan
                    {

                        //flag_sendSMS=SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:", flag_send_sms);
                        SendOneSMS(ebankDataAccess, dwDataAccess, row, "Giao dich:", flag_send_sms, err_DW, err_EB);
                        
                        countSend++;
                    }
                    //if (flag_sendSMS == 1 || flag_sendSMSRe == 1)
                    //    err_DW++;
                    //if (flag_sendSMS == 2 || flag_sendSMSRe == 2)
                    //    err_EB++;
                    //if (flag_sendSMS == 4 || flag_sendSMSRe == 4)
                    //    err_mapcard++;
                     tol_err_DW =+ err_DW;
                     tol_err_EB = +err_EB;
                }
                if (tol_err_DW > 0)
                {
                    SendSMSForDW_FPT(mobile, err_DW);
                    SendSMSForDW_FPT(mobile2, err_DW);
                    SendSMSForDW_FPT(mobile3, err_DW);
                    //SendSMSForDW_FPT(mobile4, err_DW);
                    SendSMSForDW_FPT(mobile7, err_DW);
                }
                if (tol_err_EB > 0)
                {
                    SendSMSForGW_EB(mobile, err_EB);
                    SendSMSForGW_EB(mobile2, err_EB);
                    SendSMSForGW_EB(mobile3, err_EB);
                    //SendSMSForGW_EB(mobile4, err_EB);
                    SendSMSForGW_EB(mobile7, err_EB);
                }
                //if (err_mapcard > 0)
                //{
                //    SendSMSForLossMapCard(mobile, err_mapcard);
                //    SendSMSForLossMapCard(mobile2, err_mapcard);
                //    SendSMSForLossMapCard(mobile3, err_mapcard);
                //    SendSMSForLossMapCard(mobile4, err_mapcard);
                //    SendSMSForLossMapCard(mobile7, err_mapcard);
                //}
                ebankDataAccess.CloseConnection();
                dwDataAccess.CloseConnection();
                classGDMCDebitLogWriter.WriteLog("Message giao dich thuan da duoc Insert vao EbankGW thanh cong: " + countSend);
                classGDMCDebitLogWriter.WriteLog("Message giao dich reversal da duoc Insert vao EbankGW thanh cong: " + countReversalSend);
                return;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error Insert_SMSMessage() " + ex.Message);
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
                classGDMCDebitLogWriter.WriteLog("Error IsExistedSMSOnDW() " + ex.Message);
                return false;
            }
        }

        private static DataTable Get_GDMCDebit()
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
                    maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);//hoannd change 20/04/2015
                   

                }
                
                if (maxUpdateDT != null)//hoannd add 17/04/2015
                    table = _dataAccess.GetGDMCDebit(maxUpdateDT);
                    return table;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error Get_GDMCDebit() 1, " + ex.Message);
                table.Rows.Clear();
                return table;
            }
           
        }
        private static string CreateSMSDebitMessage(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchaneName,string vip_card, string vip_cif)
        {
            try
            {
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string smsMessage = "SCB hoan tra " + amount1 + crncyAlpha + " cho the C-" + brand + " Debit x" + pan
                + " do GD khong thanh cong tai " + merchaneName + " luc " + time + " " + date + ". De biet them chi tiet LH " + SCBPhone;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error CreateSMSDebitMessage(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSCreditMessage(string brand, string pan, string date, string time,
                                        string amount, string crncyCode, string merchaneName, string vip_card, string vip_cif)
        {
            try
            {
                if (vip_card == "Y" || vip_cif=="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string smsMessage = "Cam on Quy khach da su dung the SCB C-" + brand + " Debit x" + pan + " giao dich thanh cong " + amount1 + crncyAlpha
                + ". Luc " + time + " " + date + ". Tai " + merchaneName + ". Chi tiet LH: " + SCBPhone;               

                return smsMessage;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error CreateSMSCreditMessage(), " + ex.Message);
                return "";
            }
        }


        
    }
}
