using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;

namespace SendSMSForMC
{
    class classThuNoTatToanIPP
    {
        public static bool exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();
        //private static string[][] _currencyMapping = new string[300][];
        //private static string DEFAULT_CRNCY_ALPA = "MTT";
        public static string SMS_TYPE = "TNTTIP";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int value = 0;
            int minute = 0;
            DataTable table = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("ThuNoVaTatToanIPP");

            while (exitThread == false)
            {
                minute = DateTime.Now.Minute;
                if (minute % value == 0)
                //if(1==1)//hhhh
                {
                    try
                    {
                        classThuNoTatToanIPPLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_ThuNo_TatToan_IPP();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classThuNoTatToanIPPLogWriter.WriteLog("----------------End Process-----------------");
                        
                    }
                    catch (Exception ex)
                    {
                        classThuNoTatToanIPPLogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }

                if (value > 2)
                {
                    if ((value - (minute % value) - 1) > 0)
                    {
                        classThuNoTatToanIPPLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
                        Thread.Sleep(1000 * (value - (minute % value) - 1) * 55);
                    }
                    else
                    {
                        Thread.Sleep(1000 * 10);// truong hop start vao -1 ph
                    }
                }               
                else
                    Thread.Sleep(1000 * 55); // 10 giay moi check 1 lan
            }
        }

        private static DataTable Get_ThuNo_TatToan_IPP()
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
                    classThuNoTatToanIPPLogWriter.WriteLog("Error Get_ThuNo_TatToan_IPP(), " + ex.Message);
                    return null;
                }
                table = _dataAccess.GetThuNoTatToanIPP(maxUpdateDT);
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
            int count_succ = 0;       
            int count_err_dw = 0;
            int count_err_eb = 0;
            string mobile = classUtilities.GetStringValueFromConfig("MyPhone1");
            string mobile2 = classUtilities.GetStringValueFromConfig("MyPhone2");
            string mobile3 = classUtilities.GetStringValueFromConfig("MyPhone3");
            string mobile4 = classUtilities.GetStringValueFromConfig("MyPhone4");
            string mobile7 = classUtilities.GetStringValueFromConfig("MyPhone7"); 
            foreach (DataRow row in table.Rows)
            {
                
               string available = "";
                if (double.Parse(row.ItemArray[12].ToString()) < double.Parse(row.ItemArray[13].ToString()))
                    available = row.ItemArray[12].ToString();
                else
                    available = row.ItemArray[13].ToString();

                if(row.ItemArray[15].ToString()=="ThanhToan")//hhhh
                    //message = CreateSMSMessageThuNo(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                    //                                row.ItemArray[6].ToString(), row.ItemArray[7].ToString(), available, row.ItemArray[17].ToString(), row.ItemArray[14].ToString(), row.ItemArray[19].ToString());
                    message = CreateSMSMessageThuNo_160(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                                                    row.ItemArray[6].ToString(), row.ItemArray[7].ToString(), available, row.ItemArray[17].ToString(), row.ItemArray[14].ToString(), row.ItemArray[19].ToString());
                else
                    message = CreateSMSMessageTatToan(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                                                    row.ItemArray[6].ToString(), row.ItemArray[18].ToString(), available, row.ItemArray[17].ToString(), row.ItemArray[14].ToString(), row.ItemArray[19].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    int flag_fpt=0;
                    int flag_eb = 0;
                    if (row.ItemArray[1].ToString() == "khong co")
                    {
                        flag_fpt = dwDataAccess.InsertThuNoTatToanIPPSMSToDW(SMS_TYPE, message
                                            , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[2].ToString()
                                            , row.ItemArray[11].ToString()
                                            , row.ItemArray[4].ToString()
                                            , row.ItemArray[3].ToString()
                                            , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                            , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                            , row.ItemArray[8].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                            , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                            , row.ItemArray[9].ToString()
                                            , "Y"
                                            ,row.ItemArray[16].ToString()
                                                            );
                        
                    }
                    else
                    {
                        flag_fpt = dwDataAccess.InsertThuNoTatToanIPPSMSToDW(SMS_TYPE, message
                                            , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[2].ToString()
                                            , row.ItemArray[11].ToString()
                                            , row.ItemArray[4].ToString()
                                            , row.ItemArray[3].ToString()
                                            , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                            , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                            , row.ItemArray[8].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                            , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                            , row.ItemArray[9].ToString()
                                            , "N"
                                            , row.ItemArray[16].ToString()
                                                            );
                    }
                    if (flag_fpt == 0)
                    {
                        classThuNoTatToanIPPLogWriter.WriteLog("card no " + row.ItemArray[11].ToString() + " can't insert DB DW");
                        count_err_dw++;
                    }
                    else
                    {


                        //mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[2].ToString(), row.ItemArray[1].ToString());
                        if (row.ItemArray[1].ToString() == "khong co")
                        {
                            flag_eb = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                row.ItemArray[10].ToString()
                                                                , row.ItemArray[1].ToString()//classUtilities.GetRandomMobile()
                                                                , message
                                                                , 'Y'//Y: se ko gui tin nhan),//N: se gui tin nhan
                                                                , SMS_TYPE);
                        }
                        else
                        {
                            string mobile_spe = classUtilities.GetMobileFromCardNoOfSpecialList2(row.ItemArray[11].ToString());
                            if (mobile_spe == "000")// the phu khong thuoc ds dac biet
                            {
                                flag_eb = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                               row.ItemArray[10].ToString()
                                                               , row.ItemArray[1].ToString()//classUtilities.GetRandomMobile()
                                                               , message
                                                               , 'N'//Y: se ko gui tin nhan),//N: se gui tin nhan
                                                               , SMS_TYPE);
                            }
                            else //the phu thuoc ds dac biet
                            {
                                flag_eb = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                              row.ItemArray[10].ToString()
                                                              , mobile_spe
                                                              , message
                                                              , 'N'//Y: se ko gui tin nhan),//N: se gui tin nhan
                                                              , SMS_TYPE);
                            }
                           
                        }

                        if (flag_eb == 0)
                        {
                            classThuNoTatToanIPPLogWriter.WriteLog("mobi no " + row.ItemArray[1].ToString() + " can't insert DB EB");
                            classThuNoTatToanIPPLogWriter.WriteLog("card no " + row.ItemArray[11].ToString() + " can't insert DB EB");
                            dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                        row.ItemArray[11].ToString(),
                                        row.ItemArray[5].ToString(),                                       
                                        row.ItemArray[16].ToString(),
                                        "E"
                                        );
                            count_err_eb++;
                        }
                        else
                            count_succ++;
                    }
                   
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            if (count_err_dw > 0)
            {
                SendSMSForDW_FPT(mobile, count_err_dw);
                SendSMSForDW_FPT(mobile2, count_err_dw);
                SendSMSForDW_FPT(mobile3, count_err_dw);
                SendSMSForDW_FPT(mobile4, count_err_dw);
                SendSMSForDW_FPT(mobile7, count_err_dw);
            }
            if (count_err_eb > 0)
            {
                SendSMSForGW_EB(mobile, count_err_eb);
                SendSMSForGW_EB(mobile2, count_err_eb);
                SendSMSForGW_EB(mobile3, count_err_eb);
                SendSMSForGW_EB(mobile4, count_err_eb);
                SendSMSForGW_EB(mobile7, count_err_eb);
            }

            classThuNoTatToanIPPLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count_succ);
            classThuNoTatToanIPPLogWriter.WriteLog("Message loi khong duoc Insert vao EbankGW: " + count_err_eb);
            classThuNoTatToanIPPLogWriter.WriteLog("Message loi khong duoc Insert vao DW: " + count_err_dw);
            return;
        }
        private static string CreateSMSMessageTatToan(string brand, string pan, string date, string time, string amount, string ava_bal, string merchant, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(6, '0'); // vi co truong hop chi co 5 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string amount_t = string.Format("{0:#,##0.##}", double.Parse(amount));
                string ava_bal_t = string.Format("{0:#,##0.##}", double.Parse(ava_bal));
                string smsMessage = "Quy khach da tat toan tra gop " + amount_t + "VND tai " + merchant + " thanh cong cho the SCB " + brand + " x" + pan + " luc "+time+ " ngay " + date; 
                if (vip_card == "Y" || vip_cif == "Y")
                    smsMessage = smsMessage + ". Lien he 1800545438";
                else
                    smsMessage = smsMessage + ". Lien he 19006538";
                return smsMessage;
            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error CreateSMSMessageTatToan(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessageThuNo_160(string brand, string pan, string date, string time, string amount, string ava_bal, string merchant, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(6, '0'); // vi co truong hop chi co 5 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string amount_t = string.Format("{0:#,##0.##}", double.Parse(amount));
                string ava_bal_t = string.Format("{0:#,##0.##}", double.Parse(ava_bal));
                string hotline = "";
                if (vip_card == "Y" || vip_cif == "Y")
                    hotline= "\nLH 1800545438";
                else
                    hotline= "\nLH 19006538";
                string smsMessage = "Quy khach da thanh toan tra gop " + amount_t + "VND cho the SCB x" + pan + "\nGD tai " + merchant + "\nNgay " + date + hotline;
                if (smsMessage.Length > 160)
                {
                    if (merchant.Length > (smsMessage.Length - 160))
                    {
                        string new_marchant = merchant.Substring(0, merchant.Length - (smsMessage.Length - 160));
                        smsMessage = "Quy khach da thanh toan tra gop " + amount_t + "VND cho the SCB x" + pan + "\nGD tai " + merchant + "\nNgay " + date + hotline;
                    }
                }
                return smsMessage;

            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error CreateSMSMessageThuNo_160(), " + ex.Message);
                return "";
            }
        }

        private static string CreateSMSMessageThuNo(string brand, string pan, string date, string time, string amount, string ava_bal,string merchant, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(6, '0'); // vi co truong hop chi co 5 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);                
                string amount_t = string.Format("{0:#,##0.##}", double.Parse(amount));
                string ava_bal_t = string.Format("{0:#,##0.##}", double.Parse(ava_bal));
                string smsMessage = "Quy khach da thanh toan tra gop " + amount_t + "VND tai " + merchant + " cho the SCB " + brand + " x" + pan + " ngay " + date; //". Han muc kha dung cua the: " + ava_bal_t 
                if(vip_card=="Y" || vip_cif == "Y")
                    smsMessage=smsMessage+". Lien he 1800545438";
                else
                    smsMessage=smsMessage+". Lien he 19006538";
                return smsMessage;
            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error CreateSMSMessageThuNo(), " + ex.Message);
                return "";
            }
        }
        private static void SendSMSForDW_FPT(string mobile, int err)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check table FPT.smsmastercard, " + err + " SMS can't insert DB DW for ThuNoTatToanIPP.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error SendSMSForDW_FPT():" + ex.Message);
            }
        }
        private static void SendSMSForGW_EB(string mobile, int err)
        {
            try
            {

                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "pls check connect to Ebanking GW," + err + " SMS can't insert data for ThuNoTatToanIPP.";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error SendSMSForGW_EB():" + ex.Message);
            }
        }
    }
}
