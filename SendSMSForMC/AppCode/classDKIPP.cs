using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;
namespace SendSMSForMC.AppCode
{
    class classDKIPP
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "DKIPP";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int value = 0;
            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("DKIPP_Minute");
            while (_exitThread == false)
            {
                try
                {
                    minute = DateTime.Now.Minute;

                    if (minute % value == 0)                    {
                       
                        classDKIPPLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_DKIPP();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classDKIPPLogWriter.WriteLog("----------------End Process-----------------");
                        
                    }
                    if (value > 2)
                    {
                        if ((value - (minute % value) - 1) > 0)
                        {
                            classDKIPPLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
                            Thread.Sleep(1000 * (value - (minute % value) - 1) * 55);
                        }
                        else
                        {
                            Thread.Sleep(1000 * 10);// truong hop start vao -1 ph
                        }
                    }
                    else
                        Thread.Sleep(1000 * 55);
                }
                catch (Exception ex)
                {
                    classDKIPPLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_DKIPP()
        {
            string maxUpdateDT = null;
            DataTable table = new DataTable();
            if (string.IsNullOrEmpty(_updateDateTime) == false)
            {
                maxUpdateDT = _updateDateTime;
                _updateDateTime = null;
            }
            else
            {
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
               
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classDKIPPLogWriter.WriteLog("Error Get_DKIPP(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetDKIPP(maxUpdateDT);
            }
            return table;
        }


        private static void Insert_SMSMessage(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int result = 0;
            int succ_eb = 0;
            int err_eb = 0;
            int succ_dw = 0;
            int err_dw = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                result = 0;

                //message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(),
                //                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                //                                        row.ItemArray[8].ToString(), row.ItemArray[9].ToString(),
                //                                        row.ItemArray[16].ToString(), row.ItemArray[17].ToString()
                //                                        ,row.ItemArray[19].ToString()
                //                                        );
                message = CreateSMSMessage_160(row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(),
                                                        row.ItemArray[6].ToString(), row.ItemArray[7].ToString(),
                                                        row.ItemArray[8].ToString(), row.ItemArray[9].ToString(),
                                                        row.ItemArray[16].ToString(), row.ItemArray[17].ToString()
                                                        , row.ItemArray[19].ToString()
                                                        );
                if (string.IsNullOrEmpty(message) == false)
                {
                    if (row.ItemArray[18].ToString().Trim() == "")//giao dich cua the chinh, phone the phu se khong co
                    {
                        //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[15].ToString(), row.ItemArray[10].ToString());
                        if (row.ItemArray[10].ToString() == "khong co")                        
                        {
                            int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                   SMS_TYPE
                                                                   , message
                                                                   , row.ItemArray[10].ToString()
                                                                   , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                   , row.ItemArray[15].ToString()
                                                                   , row.ItemArray[2].ToString()
                                                                   , row.ItemArray[3].ToString()
                                                                   , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                   , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                   , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                   , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                   , row.ItemArray[9].ToString()
                                                                   , row.ItemArray[11].ToString()
                                                                   , row.ItemArray[12].ToString()
                                                                   , row.ItemArray[1].ToString()
                                                                   , "Y"
                                                                   );
                            if (flag_fpt == 0)
                            {
                                classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                err_dw++;

                            }
                            else
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                               row.ItemArray[14].ToString()
                                                                               , row.ItemArray[10].ToString()
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                                if (result == 0)
                                {
                                    err_eb++;
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                }
                                else
                                    succ_eb++;
                            }
                        }
                        else // so phone hop le
                        {
                            int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                SMS_TYPE
                                                                , message
                                                                , row.ItemArray[10].ToString()
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                , row.ItemArray[15].ToString()
                                                                , row.ItemArray[2].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[11].ToString()
                                                                , row.ItemArray[12].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "N" //hhhh
                                                                );
                            if (flag_fpt == 0)
                            {
                                classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                err_dw++;

                            }
                            else
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                               row.ItemArray[14].ToString()
                                                                               , row.ItemArray[10].ToString()
                                                                               , message
                                                                               , 'N'//hhhh Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                                if (result == 0)
                                {
                                    dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                            row.ItemArray[15].ToString(),
                                            row.ItemArray[5].ToString(),
                                            row.ItemArray[11].ToString(),
                                            "E"
                                            );
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                    err_eb++;
                                }
                                else
                                    succ_eb++;
                            }
                        }
                    }
                    else // giao dich the phu
                    {
                        string mobile = classUtilities.GetMobileFromCardNoOfSpecialList2(row.ItemArray[15].ToString());
                        if (mobile == "000")//the phu khong thuoc ds dac biet
                        {
                            if (row.ItemArray[18].ToString() == "khong co") //phone the phu
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                       SMS_TYPE
                                                                       , message
                                                                       , row.ItemArray[18].ToString()
                                                                       , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                       , row.ItemArray[15].ToString()
                                                                       , row.ItemArray[2].ToString()
                                                                       , row.ItemArray[3].ToString()
                                                                       , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                       , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                       , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                       , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                       , row.ItemArray[9].ToString()
                                                                       , row.ItemArray[11].ToString()
                                                                       , row.ItemArray[12].ToString()
                                                                       , row.ItemArray[1].ToString()
                                                                       , "Y"
                                                                       );                                
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[14].ToString()
                                                                                   , row.ItemArray[18].ToString()
                                                                                   , message
                                                                                   , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        err_eb++;
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                    }
                                    else
                                        succ_eb++;
                                }

                                
                            }
                            else // so phone hop le
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                    SMS_TYPE
                                                                    , message
                                                                    , row.ItemArray[18].ToString()
                                                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                    , row.ItemArray[15].ToString()
                                                                    , row.ItemArray[2].ToString()
                                                                    , row.ItemArray[3].ToString()
                                                                    , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                    , row.ItemArray[9].ToString()
                                                                    , row.ItemArray[11].ToString()
                                                                    , row.ItemArray[12].ToString()
                                                                    , row.ItemArray[1].ToString()
                                                                    , "N" //hhhh
                                                                    );
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[14].ToString()
                                                                                   , row.ItemArray[18].ToString()
                                                                                   , message
                                                                                   , 'N'//hhhh Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                                row.ItemArray[15].ToString(),
                                                row.ItemArray[5].ToString(),
                                                row.ItemArray[11].ToString(),
                                                "E"
                                                );
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                        err_eb++;
                                    }
                                    else
                                        succ_eb++;
                                }
                            }
                            if (row.ItemArray[10].ToString() == "khong co") //phone the chinh
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                       SMS_TYPE
                                                                       , message
                                                                       , row.ItemArray[10].ToString()
                                                                       , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                       , row.ItemArray[15].ToString()
                                                                       , row.ItemArray[2].ToString()
                                                                       , row.ItemArray[3].ToString()
                                                                       , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                       , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                       , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                       , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                       , row.ItemArray[9].ToString()
                                                                       , row.ItemArray[11].ToString()
                                                                       , row.ItemArray[12].ToString()
                                                                       , row.ItemArray[1].ToString()
                                                                       , "Y"
                                                                       );
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[18].ToString()
                                                                                   , row.ItemArray[10].ToString()
                                                                                   , message
                                                                                   , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        err_eb++;
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                    }
                                    else
                                        succ_eb++;
                                }


                            }
                            else // so phone hop le
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                    SMS_TYPE
                                                                    , message
                                                                    , row.ItemArray[10].ToString()
                                                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                    , row.ItemArray[15].ToString()
                                                                    , row.ItemArray[2].ToString()
                                                                    , row.ItemArray[3].ToString()
                                                                    , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                    , row.ItemArray[9].ToString()
                                                                    , row.ItemArray[11].ToString()
                                                                    , row.ItemArray[12].ToString()
                                                                    , row.ItemArray[1].ToString()
                                                                    , "N" //hhhh
                                                                    );
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[14].ToString()
                                                                                   , row.ItemArray[10].ToString()
                                                                                   , message
                                                                                   , 'N'//hhhh Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                                row.ItemArray[15].ToString(),
                                                row.ItemArray[5].ToString(),
                                                row.ItemArray[11].ToString(),
                                                "E"
                                                );
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                        err_eb++;
                                    }
                                    else
                                        succ_eb++;
                                }
                            }

                        }
                        else // the phu thuoc ds dac biet
                        {
                            if (row.ItemArray[10].ToString() == "khong co")
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                       SMS_TYPE
                                                                       , message
                                                                       , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                       , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                       , row.ItemArray[15].ToString()
                                                                       , row.ItemArray[2].ToString()
                                                                       , row.ItemArray[3].ToString()
                                                                       , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                       , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                       , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                       , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                       , row.ItemArray[9].ToString()
                                                                       , row.ItemArray[11].ToString()
                                                                       , row.ItemArray[12].ToString()
                                                                       , row.ItemArray[1].ToString()
                                                                       , "Y"
                                                                       );
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[14].ToString()
                                                                                   , mobile
                                                                                   , message
                                                                                   , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        err_eb++;
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                    }
                                    else
                                        succ_eb++;
                                }
                            }
                            else // so phone hop le
                            {
                                int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(
                                                                    SMS_TYPE
                                                                    , message
                                                                    , mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                    , row.ItemArray[15].ToString()
                                                                    , row.ItemArray[2].ToString()
                                                                    , row.ItemArray[3].ToString()
                                                                    , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                    , row.ItemArray[9].ToString()
                                                                    , row.ItemArray[11].ToString()
                                                                    , row.ItemArray[12].ToString()
                                                                    , row.ItemArray[1].ToString()
                                                                    , "N" //hhhh
                                                                    );
                                if (flag_fpt == 0)
                                {
                                    classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB DW");
                                    err_dw++;

                                }
                                else
                                {
                                    result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                   row.ItemArray[14].ToString()
                                                                                   , mobile
                                                                                   , message
                                                                                   , 'N'//hhhh Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                                    if (result == 0)
                                    {
                                        dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                                row.ItemArray[15].ToString(),
                                                row.ItemArray[5].ToString(),
                                                row.ItemArray[11].ToString(),
                                                "E"
                                                );
                                        classDKIPPLogWriter.WriteLog("err: card no " + row.ItemArray[15].ToString() + " can't insert DB EB");
                                        err_eb++;
                                    }
                                    else
                                        succ_eb++;
                                }
                            }
                        }
                    }
                    //
    
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classDKIPPLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + succ_eb);
            classDKIPPLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + err_eb);
            classDKIPPLogWriter.WriteLog("Message da duoc Insert vao DW thanh cong: " + succ_dw);
            classDKIPPLogWriter.WriteLog("Message loi khong Insert vao DW: " + err_dw);
            return;
        }

        private static string CreateSMSMessage_160(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchant, string vip_card, string vip_cif, string EASYCASH)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                //string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string hot_line = "";
                string smsMessage = "";
                if (vip_card == "Y" || vip_cif == "Y")
                    hot_line = "1800545438";
                else
                    hot_line = "19006538";
                if (EASYCASH == "EC")//la giao dich tra gop EASYCASH
                {
                    smsMessage = "Khoan rut tien Easy Cash tu the " + pan + " cua Quy khach da duoc chap nhan\nTien se duoc chuyen vao TKTT cua Quy khach tai SCB trong vong 2 ngay lam viec\nLH " + hot_line;
                }
                else
                {
                    smsMessage = "The SCB x" + pan + " vua moi chuyen doi tra gop thanh cong "
                    + amount1 + "VND\nTai " + merchant +"\nLuc "+ time + " ngay " + date + "\nLH: " + hot_line;
                    if (smsMessage.Length > 160)
                    {
                        if (merchant.Length > (smsMessage.Length - 160))
                        {
                            string new_marchant = merchant.Substring(0, merchant.Length - (smsMessage.Length - 160));
                            smsMessage = "The SCB x" + pan + " vua moi chuyen doi tra gop thanh cong "
                            + amount1 + "VND\nTai " + new_marchant + "\nLuc " + time + " ngay " + date + "\nLH: " + hot_line;
                        }
                    }
                }
                return smsMessage;
            }
            catch (Exception ex)
            {
                classDKIPPLogWriter.WriteLog("Error CreateSMSMessage_160(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage(string brand, string pan, string date, string time,
                                       string amount, string crncyCode, string merchant,string vip_card, string vip_cif,string EASYCASH)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(8, '0'); // vi co truong hop chi co 7 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                //string crncyAlpha = classUtilities.ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string hot_line = "";
                string smsMessage = "";
                if (vip_card == "Y" || vip_cif =="Y")
                    hot_line = "1800545438";
                else
                    hot_line = "19006538";
                if (EASYCASH == "EC")//la giao dich tra gop EASYCASH
                {
                    smsMessage = "Khoan rut tien Easy Cash tu the " + pan + " cua Quy khach da duoc chap nhan,tien se duoc chuyen vao TKTT cua Quy khach tai SCB trong 2 ngay lam viec.LH " + hot_line;
                }
                else
                {
                    smsMessage = "Cam on Quy khach da su dung the SCB " + brand + " x" + pan + " chuyen doi tra gop thanh cong "
                        + amount1 + "VND Luc " + time + " ngay " + date + " tai " + merchant + " .Chi tiet LH: " + hot_line;
                }
                return smsMessage;
            }
            catch (Exception ex)
            {
                classDKIPPLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
