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
    class classOutstandingBalancePaymentManual
    {
        public static bool exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();
        //private static string[][] _currencyMapping = new string[300][];
        //private static string DEFAULT_CRNCY_ALPA = "MTT";
        public static string SMS_TYPE = "MANPAY";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int value = 0;
            int minute = 0;
            DataTable table = new DataTable();

            //SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Manual_Minute");

            while (exitThread == false)
            {
                minute = DateTime.Now.Minute;
                if (minute % value == 0)
                //if(1==1)//hhhh
                {
                    try
                    {
                        classOutstandingBalancePaymentManualLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_OutBal_Payment_Manual();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classOutstandingBalancePaymentManualLogWriter.WriteLog("----------------End Process-----------------");
                        
                    }
                    catch (Exception ex)
                    {
                        classOutstandingBalancePaymentManualLogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }

                if (value > 2)
                {
                    if ((value - (minute % value) - 1) > 0)
                    {
                        classOutstandingBalancePaymentManualLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
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

        private static DataTable Get_OutBal_Payment_Manual()
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
                /*
                DataTable updateTime = _dataAccess.Get_Max_OutBal_UpdateTime(SMS_TYPE);
                if (updateTime.Rows.Count == 1)
                    maxUpdateDT = updateTime.Rows[0].ItemArray[0].ToString();
                 */
                //maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime_Pay(SMS_TYPE);
                
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classOutstandingBalancePaymentManualLogWriter.WriteLog("Error Get_OutBal_Payment_Manual(), " + ex.Message);
                    return null;
                }
                table = _dataAccess.GetPaymentManual(maxUpdateDT);
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
            //string mobile4 = classUtilities.GetStringValueFromConfig("MyPhone4");
            string mobile7 = classUtilities.GetStringValueFromConfig("MyPhone7"); 
         
            foreach (DataRow row in table.Rows)
            {
                
               string available = "";
                if (double.Parse(row.ItemArray[12].ToString()) < double.Parse(row.ItemArray[13].ToString()))
                    available = row.ItemArray[12].ToString();
                else
                    available = row.ItemArray[13].ToString();
                message = CreateSMSMessage(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                                                    row.ItemArray[6].ToString(), row.ItemArray[7].ToString(), available, row.ItemArray[15].ToString(), row.ItemArray[16].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    int flag_fpt=0;
                    int flag_eb = 0;
                    string phone="";
                    if(row.ItemArray[17].ToString()!="")// la the phu(crn the phu khac null)
                    {
                        if(row.ItemArray[18].ToString()=="1")//the phu co gui sms cho the chinh
                            phone=row.ItemArray[1].ToString();//gui cho the chinh
                        else
                            phone=row.ItemArray[19].ToString();//gui cho the phu
                    }
                    else
                        phone=row.ItemArray[1].ToString();
                    //if (row.ItemArray[1].ToString() == "khong co")
                    if (phone == "khong co" || phone=="")
                    {
                        flag_fpt = dwDataAccess.InsertPaymnetManualSMSToDW(SMS_TYPE, message
                                            , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[2].ToString()
                                            , row.ItemArray[11].ToString()
                                            , row.ItemArray[4].ToString()
                                            , row.ItemArray[3].ToString()
                                            , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                            , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                            , row.ItemArray[21].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                            , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                            , row.ItemArray[9].ToString()
                                            , "Y"
                                            , row.ItemArray[14].ToString()
                                            , row.ItemArray[22].ToString()
                                            , row.ItemArray[23].ToString()
                                            , row.ItemArray[24].ToString()
                                            , row.ItemArray[20].ToString()
                                            , row.ItemArray[13].ToString()
                                            , row.ItemArray[12].ToString()
                                                            );
                        
                    }
                    else // so phone hop le
                    {
                        flag_fpt = dwDataAccess.InsertPaymnetManualSMSToDW(SMS_TYPE, message
                                            , phone
                                            , DateTime.Parse(row.ItemArray[0].ToString())
                            //, row.ItemArray[2].ToString()
                                            , row.ItemArray[11].ToString()
                                            , row.ItemArray[4].ToString()
                                            , row.ItemArray[3].ToString()
                                            , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                            , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                            , row.ItemArray[21].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                            , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                            , row.ItemArray[9].ToString()
                                            , "N"
                                            , row.ItemArray[14].ToString()
                                            , row.ItemArray[22].ToString()
                                            , row.ItemArray[23].ToString()
                                            , row.ItemArray[24].ToString()
                                            , row.ItemArray[20].ToString()
                                            , row.ItemArray[13].ToString()
                                            , row.ItemArray[12].ToString()
                                                            );
                    }
                    if (flag_fpt == 0)
                    {
                        classOutstandingBalancePaymentManualLogWriter.WriteLog("card no " + row.ItemArray[11].ToString() + " can't insert DB DW");
                        count_err_dw++;
                    }
                    else// insert DW thanh cong
                    {


                       
                        //if (row.ItemArray[1].ToString() == "khong co")
                        if (phone == "khong co" || phone == "")
                        {
                            flag_eb = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                                row.ItemArray[10].ToString()
                                                                , phone
                                                                , message
                                                                , 'Y'//Y: se ko gui tin nhan),//N: se gui tin nhan
                                                                , SMS_TYPE);
                        }
                        else
                        {
                           
                           flag_eb = ebankDataAccess.InsertSMSMessateToEBankGW(//classDataAccess.IDALERT
                                                              row.ItemArray[10].ToString()
                                                              , phone
                                                              , message
                                                              , 'N'//hhhh Y: se ko gui tin nhan),//N: se gui tin nhan
                                                              , SMS_TYPE);
                        }

                        if (flag_eb == 0)
                        {
                            classOutstandingBalancePaymentManualLogWriter.WriteLog("mobile no " + row.ItemArray[1].ToString() + " can't insert DB EB");
                            dwDataAccess.Update_Status_SMS(//update status sms ve loi ko gui qua EW
                                        row.ItemArray[11].ToString(),
                                        row.ItemArray[5].ToString(),
                                        row.ItemArray[14].ToString(),
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
                //SendSMSForDW_FPT(mobile4, count_err_dw);
                SendSMSForDW_FPT(mobile7, count_err_dw);
            }
            if (count_err_eb > 0)
            {
                SendSMSForGW_EB(mobile, count_err_eb);
                SendSMSForGW_EB(mobile2, count_err_eb);
                SendSMSForGW_EB(mobile3, count_err_eb);
                //SendSMSForGW_EB(mobile4, count_err_eb);
                SendSMSForGW_EB(mobile7, count_err_eb);
            }
            
            classOutstandingBalancePaymentManualLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count_succ);
            classOutstandingBalancePaymentManualLogWriter.WriteLog("Message loi khong duoc Insert vao EbankGW: " + count_err_eb);
            classOutstandingBalancePaymentManualLogWriter.WriteLog("Message loi khong duoc Insert vao DW: " + count_err_dw);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string date, string time, string amount, string ava_bal, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(6, '0'); // vi co truong hop chi co 5 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);                
                string amount_t = string.Format("{0:#,##0.##}", double.Parse(amount));
                string ava_bal_t = string.Format("{0:#,##0.##}", double.Parse(ava_bal));
                if (vip_card == "Y"||vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                string smsMessage = "Cam on Quy khach da thanh toan " + amount_t + "VND vao the SCB X" + pan + "\nNgay " + date + "\nHan muc kha dung cua the: " + ava_bal_t + "\nChi tiet: " + SCBPhone;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentManualLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
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
                string message = "pls check table FPT.smsmastercard, " + err + " SMS can't insert DB DW for PaymentManual.";

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
                string message = "pls check connect to Ebanking GW," + err + " SMS can't insert data for PaymentManual.";

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
    }
}
