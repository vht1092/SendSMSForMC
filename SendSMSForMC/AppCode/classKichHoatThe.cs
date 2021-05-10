using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using SendSMSForMC.AppCode;

namespace SendSMSForMC.AppCode
{
    class classKichHoatThe
    {
        public static bool _exitThread = false;
        public static string _updateDataTime = null;
        private static classDataAccess _dataAccess = new classDataAccess();
        public static string SMS_TYPE ="GDKHT";
        private static string SCBPhone="";
        private static string zpk_uat = "9DB6EF5E73F71AD36420C4B0E0EFCBBA91F1F8B60B0BF49E";
        private static string zpk_live = "4C19928620F192BAE683ADDFC1BF7A5BE6805BE98C9EB39D";
        public static void RunService()    
        {
            int minute =0;
            int value = 0;
            DataTable table = new DataTable();
            SCBPhone= classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("GDKichHoatThe_Minute");
            while (_exitThread == false)
            {
                minute = DateTime.Now.Minute;
                //if (minute % value == 0)//hhhh             
                if(1==1)
                {
                    try
                    {
                        classKichHoatTheLogWriter.WriteLog("-------------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_KichHoatThe();
                        //the nao kich hoat, the do nhan sms
                        if (table.Rows.Count > 0)
                        {
                            Insert_SMSMessage(table);
                        }
                        classKichHoatTheLogWriter.WriteLog("----------------End Process-----------------");
                        if (value > 2)
                        Thread.Sleep(1000 * (value - 2) * 55);// sleep (value -1) phut de troi qua thoi gian lap lai

                    }
                    catch (Exception ex)
                    {
                        classKichHoatTheLogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }
                Thread.Sleep(1000 * 55);
            }

        }

        public static DataTable Get_KichHoatThe()
        {
            string maxUpdateDT = null;
            DataTable table = new DataTable();
            if (string.IsNullOrEmpty(_updateDataTime) == false)
            {
                maxUpdateDT = _updateDataTime;
                _updateDataTime = null;
            }
            else
            {
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
               
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long MaxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classKichHoatTheLogWriter.WriteLog("Error Get_KichHoatThe(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetKichHoatThe(maxUpdateDT);
            }
            return table;
        }
        private static void Insert_SMSMessage(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            classDataAccess IMDataAccess = new classDataAccess();            

            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");
            IMDataAccess.OpenConnection("CW_IM");
            string link_km = classUtilities.GetStringValueFromConfig("SCB_KM");
            string link_HDSD = classUtilities.GetStringValueFromConfig("SCB_HDSD");
            string message = "";
            string message_encode = "";
            int result = 0;
            int count = 0;
            int count_err = 0;
            
            foreach (DataRow row in table.Rows)
            {               
                string card_no = classUtilities.GetCardFromSMS_Pin_List(row.ItemArray[9].ToString());
                if (card_no == "")//nhan pin giay
                {
                    string count_act = IMDataAccess.Get_First_Activate(row.ItemArray[9].ToString());
                    if (count_act == "1")//1: active lan dau
                    {
                        string count_rep = IMDataAccess.Get_Reply_Card(row.ItemArray[9].ToString());
                        if (count_rep == "0")//0: the chua thay the
                        {
                            message = CreateSMSMessage_act_first(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, link_HDSD);
                        }
                        else
                        {
                            message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km);
                        }
                    }
                    else
                    {
                        message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km);
                    }

                    //message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km);
                    if (string.IsNullOrEmpty(message) == false)
                    {

                        string mobile = row.ItemArray[7].ToString();
                        if (row.ItemArray[7].ToString() == "khong co")
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                               row.ItemArray[8].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        else
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                               row.ItemArray[8].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        if (result == 1)
                        {
                            if (row.ItemArray[7].ToString() == "khong co")
                            {
                                count += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "Y"
                                );
                            }
                            else
                            {
                                count += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "N"
                                );
                            }

                        }
                        else
                        {
                            count_err += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "E"
                                );
                        }
                    }
                    else //gen mess loi
                    {
                        classKichHoatTheLogWriter.WriteLog("err when create mess: " + row.ItemArray[9].ToString());
                    }
                }
                else //nhan pin sms
                {
                    result = 0;
                    string loc_pan = row.ItemArray[10].ToString() + row.ItemArray[4].ToString().Substring(12, 4);

                    //if (row.ItemArray[11].ToString() == "P")//P: the chinh
                    //{
                    //    string count_act = IMDataAccess.Get_First_Activate(row.ItemArray[9].ToString());
                    //    if (row.ItemArray[14].ToString() == "")//the ko thuoc CBNV
                    //    {
                    //        if (count_act == "1")//1: active lan dau
                    //        {
                    //            string count_rep = IMDataAccess.Get_Reply_Card(row.ItemArray[9].ToString());
                    //            if (count_rep == "0")//0: the chua thay the
                    //            {                                    
                    //                message = CreateSMSMessage_phi(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), row.ItemArray[7].ToString(), loc_pan);
                    //            }
                    //            else
                    //                message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan);
                    //        }
                    //        else
                    //        {
                    //            message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan);
                    //        }
                    //    }
                    //    else
                    //        message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan);
                    //}
                    //else//the phu
                    //{
                    //    //message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(),row.ItemArray[13].ToString());
                    //    message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan);
                    //}
                    string pin = Gen_PIN(row.ItemArray[7].ToString(), loc_pan);
                    if (pin != null)
                    {
                        message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan, pin);
                        message_encode = CreateSMSMessage_EC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km, row.ItemArray[7].ToString(), loc_pan, pin);
                    }
                    else //gen PIN Loi
                    {
                        classKichHoatTheLogWriter.WriteLog("gen PIN loi for LOC: " + row.ItemArray[10].ToString());
                    }
                    if (string.IsNullOrEmpty(message) == false)
                    {

                        string mobile = row.ItemArray[7].ToString();
                        if (row.ItemArray[7].ToString() == "khong co")
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                               row.ItemArray[8].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        else
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                               row.ItemArray[8].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        if (result == 1)
                        {
                            
                            if (row.ItemArray[7].ToString() == "khong co")
                            {
                                count += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message_encode
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "Y"
                                );
                            }
                            else
                            {
                                count += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message_encode
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "N"
                                );
                            }

                        }
                        else
                        {
                            count_err += dwDataAccess.InsertKichHoatTheSMSToDW(
                                    SMS_TYPE
                                , message_encode
                                , mobile
                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                , row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString()
                                , row.ItemArray[3].ToString()
                                , row.ItemArray[5].ToString()
                                , row.ItemArray[6].ToString()
                                , row.ItemArray[1].ToString()
                                , "E"
                                );
                        }
                    }

                }
          
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classKichHoatTheLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classKichHoatTheLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);

        }
        //private static void Insert_SMSMessage(DataTable table)
        //{
        //    classDataAccess ebankDataAccess = new classDataAccess();
        //    classDataAccess dwDataAccess = new classDataAccess();
        //    classDataAccess IMDataAccess = new classDataAccess();
        //    ebankDataAccess.OpenConnection("EBANK_GW");
        //    dwDataAccess.OpenConnection("CW_DW");
        //    IMDataAccess.OpenConnection("CW_IM");
        //    string link_km=classUtilities.GetStringValueFromConfig("SCB_KM");
        //    string message = "";
        //    int result = 0;
        //    int count = 0;
        //    int count_err = 0;
        //    foreach (DataRow row in table.Rows)
        //    {
        //        result = 0;
        //        //if(row.ItemArray[4].ToString().Substring(0,6)=="550796")//
        //        if (row.ItemArray[10].ToString().Substring(0, 1) == "2")// 2: the MC debit
        //            message = CreateSMSMessageMCDebit(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString());
        //        else
        //        {
        //            if (row.ItemArray[11].ToString() == "P")//P: the chinh
        //            {
        //                string count_act = IMDataAccess.Get_First_Activate(row.ItemArray[9].ToString());
                      
        //                if (count_act == "1")//1: active lan dau
        //                {
        //                    string count_rep = IMDataAccess.Get_Reply_Card(row.ItemArray[9].ToString());                                                   
        //                    if (count_rep == "0")//0: the chua thay the
        //                    {
        //                        //message = CreateFirstSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString());
        //                        message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km);
        //                    }
        //                    else
        //                    {
        //                        message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(),row.ItemArray[13].ToString());
        //                    }
        //                }
        //                else
        //                {
        //                    message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString());
        //                }
        //            }
        //            else//the phu
        //            {
        //                //message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(),row.ItemArray[13].ToString());
        //                message = CreateSMSMessage_QC(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(), row.ItemArray[6].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString(), link_km);
        //            }                  
                    
        //        }

        //        if (string.IsNullOrEmpty(message) == false)
        //        {
        //            //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[4].ToString(), row.ItemArray[7].ToString());
        //            string mobile = row.ItemArray[7].ToString();
        //            if (row.ItemArray[7].ToString() == "khong co")
        //            {
        //                result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
        //                                                                   row.ItemArray[8].ToString() 
        //                                                                   , mobile
        //                                                                   , message
        //                                                                   , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
        //                                                                   , SMS_TYPE);
        //            }
        //            else
        //            {
        //                result = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
        //                                                                   row.ItemArray[8].ToString() 
        //                                                                   , mobile
        //                                                                   , message
        //                                                                   , 'N'//Y se ko gui tin nhan, N se gui tin nhan
        //                                                                   , SMS_TYPE);
        //            }
        //            if (result == 1)
        //            {
        //                if (row.ItemArray[7].ToString() == "khong co")
        //                {
        //                    count += dwDataAccess.InsertKichHoatTheSMSToDW(
        //                        SMS_TYPE
        //                    , message
        //                    , mobile
        //                    , DateTime.Parse(row.ItemArray[0].ToString())
        //                        //, row.ItemArray[4].ToString()
        //                    , row.ItemArray[9].ToString()
        //                    , row.ItemArray[2].ToString()
        //                    , row.ItemArray[3].ToString()
        //                    , row.ItemArray[5].ToString()
        //                    , row.ItemArray[6].ToString()
        //                    , row.ItemArray[1].ToString()
        //                    , "Y"
        //                    );
        //                }
        //                else
        //                {
        //                    count += dwDataAccess.InsertKichHoatTheSMSToDW(
        //                        SMS_TYPE
        //                    , message
        //                    , mobile
        //                    , DateTime.Parse(row.ItemArray[0].ToString())
        //                        //, row.ItemArray[4].ToString()
        //                    , row.ItemArray[9].ToString()
        //                    , row.ItemArray[2].ToString()
        //                    , row.ItemArray[3].ToString()
        //                    , row.ItemArray[5].ToString()
        //                    , row.ItemArray[6].ToString()
        //                    , row.ItemArray[1].ToString()
        //                    , "N"
        //                    );
        //                }

        //            }
        //            else
        //            {
        //                count_err += dwDataAccess.InsertKichHoatTheSMSToDW(
        //                        SMS_TYPE
        //                    , message
        //                    , mobile
        //                    , DateTime.Parse(row.ItemArray[0].ToString())
        //                    //, row.ItemArray[4].ToString()
        //                    , row.ItemArray[9].ToString()
        //                    , row.ItemArray[2].ToString()
        //                    , row.ItemArray[3].ToString()
        //                    , row.ItemArray[5].ToString()
        //                    , row.ItemArray[6].ToString()
        //                    , row.ItemArray[1].ToString()
        //                    , "E"
        //                    );
        //            }
        //        }
        //    }
        //    ebankDataAccess.CloseConnection();
        //    dwDataAccess.CloseConnection();
        //    classKichHoatTheLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
        //    classKichHoatTheLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            
        //}
        private static string CreateFirstSMSMessage(string brand, string pan, string date, string time, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                
                string month = date.Substring(4, 2) + "/" + date.Substring(2, 2);
                if (vip_card == "Y"||vip_cif=="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string tmp_smsMessage = "Cam on Quy khach da kich hoat the SCB " + brand + " x" + pan
                + ". SCB kinh tang QK 1.000 diem thuong. Phi thuong nien (neu co) se duoc the hien tren sao ke. LH " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateFirstSMSMessage(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage_EC(string brand, string pan, string date, string time, string vip_card, string vip_cif, string link_qc, string mobile, string loc_pan, string pass)
        {
            try
            {
                pan = pan.Substring(12, 4);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                
                string tmp_smsMessage = "";
                
                    string pass_ec = _dataAccess.Get_Pass_Encode(pass);
                    string temp = pass_ec.Substring(4,12) + pass_ec.Substring(0,4);

                    if (pass_ec != null)
                    {
                        tmp_smsMessage = "Chuc mung Quy khach kich hoat thanh cong the "
                        + pan + ".Ma PIN giao dich ATM:" + temp
                        + ".Chi tieu ngay de tan huong nhieu uu dai hap dan tai " + link_qc + ".LH " + SCBPhone;
                    }
               
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessage_QC(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage_act_first(string brand, string pan, string date, string time, string vip_card, string vip_cif, string link_qc, string link_HDSD)
        {
            try
            {
                pan = pan.Substring(12, 4);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string month = date.Substring(4, 2) + "/" + date.Substring(2, 2); ;
                string tmp_smsMessage = "Quy khach da kich hoat thanh cong the " + pan
                + "\nChi tieu ngay de tan huong nhieu uu dai hap dan tai " + link_qc + "\nXem HDSD tai " + link_HDSD + "\nLH " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessage_act_first(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage(string brand, string pan, string date, string time, string vip_card, string vip_cif, string link_qc)
        {
            try
            {
                pan = pan.Substring(12, 4);
                if (vip_card == "Y"||vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string month = date.Substring(4, 2) + "/" + date.Substring(2, 2);               ;
                string tmp_smsMessage = "Chuc mung Quy khach kich hoat thanh cong the X" + pan
                + "\nChi tieu ngay de tan huong nhieu uu dai hap dan tai " + link_qc + "\nLH " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }            
        }
        private static string CreateSMSMessage_QC(string brand, string pan, string date, string time, string vip_card, string vip_cif, string link_qc, string mobile, string loc_pan, string pin)
        {
            try
            {
               

                pan = pan.Substring(12, 4);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string month = date.Substring(4, 2) + "/" + date.Substring(2, 2);
                string tmp_smsMessage = "";
                

                    tmp_smsMessage = "Chuc mung Quy khach kich hoat thanh cong the "
                    + pan + ".Ma PIN giao dich ATM:" + pin
                    + ".Chi tieu ngay de tan huong nhieu uu dai hap dan tai " + link_qc + ".LH " + SCBPhone;
               
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessage_QC(), " + ex.Message);
                return "";
            }
        }
        private static string Gen_PIN(string mobile, string loc_pan)
        {
            try
            {
                classChange_PIN Change_pin = new classChange_PIN();//1111
                Random RndNum = new Random();
                string pin = "";
                string mobile_tmp = "";
                pin = RndNum.Next(100000, 999999).ToString();

                if (mobile == "0" || mobile == "" || mobile.Length < 10)
                    mobile_tmp = "0123456789";
                else
                    mobile_tmp = mobile;
                int flag1 = Change_pin.Change_PIN(mobile_tmp, pin, zpk_live, loc_pan);
                //int flag1 = Change_pin.Change_PIN(mobile, pin, zpk_uat, loc_pan); 

                
                int flag2 = -1;
                if (flag1 == 0)//fail
                {
                    classKichHoatTheLogWriter.WriteLog("Error WS 1 gen PIN fo LOC :" + loc_pan.Substring(1, 16));
                    //flag2 = Change_pin.Change_PIN(mobile, pin, zpk_uat, loc_pan);//call ws lan 2
                    flag2 = Change_pin.Change_PIN(mobile_tmp, pin, zpk_live, loc_pan);//call ws lan 2
                    if (flag2 == 0)
                        classKichHoatTheLogWriter.WriteLog("Error WS 2 gen PIN fo LOC :" + loc_pan.Substring(1, 16));
                }
                
                return pin;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Gen_PIN(), " + ex.Message + ",mobile:" + mobile + ",loc:" + loc_pan);
                return "";
            }
        }
        private static string CreateSMSMessage_phi(string brand, string pan, string date, string time, string vip_card, string vip_cif, string mobile, string loc_pan)
        {
            try
            {
                classChange_PIN Change_pin = new classChange_PIN();//1111
                Random RndNum = new Random();
                string pin = RndNum.Next(100000, 999999).ToString();
                int flag1=Change_pin.Change_PIN(mobile, pin, zpk_uat, loc_pan);

                pan = pan.Substring(12, 4);

                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string create_date = date.Substring(6, 2) + "/" + date.Substring(4, 2); ;
                string tmp_smsMessage = "";
                int flag2 = -1;
                if (flag1 == 0)//fail
                {
                    classKichHoatTheLogWriter.WriteLog("Error WS 1 gen PIN fo LOC :" + loc_pan.Substring(1, 16));
                    flag2 = Change_pin.Change_PIN(mobile, pin, zpk_uat, loc_pan);//call ws lan 2
                    if(flag2==0)
                        classKichHoatTheLogWriter.WriteLog("Error WS 2 gen PIN fo LOC :" + loc_pan.Substring(1, 16));
                }
                if (flag1 == 1 || flag2==1)
                {
                    tmp_smsMessage = "The SCB " + brand + " x" + pan
                    + " vua kich hoat thanh cong.QK duoc tang PTN khi chi tieu du dieu kien trong 30ngay ke tu .PIN "
                    +pin
                    + create_date + ".LH " + SCBPhone;
                }
                else
                {
                    tmp_smsMessage = "The SCB " + brand + " x" + pan
                    + " vua kich hoat thanh cong.Quy Khach duoc tang phi thuong nien/qua tang hap dan khi chi tieu du dieu kien trong 30ngay ke tu "
                    + create_date + ".LH " + SCBPhone;
                }
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
       
        private static string CreateSMSMessageMCDebit(string brand, string pan, string date, string time, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string month = date.Substring(4, 2) + "/" + date.Substring(2, 2);              
                string tmp_smsMessage = "The SCB " + brand + " x" + pan
                + " da duoc kich hoat thanh cong. Phi thuong nien (neu co) duoc tru vao tai khoan thanh toan cua Quy khach. Chi tiet LH: " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error CreateSMSMessageMCDebit(), " + ex.Message);
                return "";
            }
        }

        /*public static PINSelectionRespBean PINChangeViaWS(string pinEncrypted, string mobilePhone, string locPan)
        {
            CardworksClient cw = new CardworksClient();
            string linkWS = System.Configuration.ConfigurationManager.AppSettings["linkWS"].ToString();
            //linkWS = EnCode.DeCodeToString(linkWS, _key);
            linkWS = "https://192.168.47.63:8443/cws/services/Cardworks?wsdl";
            cw.Endpoint.Address = new EndpointAddress(new Uri(linkWS));
            System.Net.ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };

            PINSelectionReqBean req = new PINSelectionReqBean();
            req.sequenceNo = generateNumber();
            req.fi = "970429"; //Hard code
            req.pan = locPan; //LOC + 4 last digit Pan
            req.actInd = "3"; //Hard code
            req.mobileNo = mobilePhone;
            req.newPIN = pinEncrypted;

            PINSelectionRespBean resp = cw.PINSelection(req);
            return resp;
        }*/

    }
}
