using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using SendSMSForMC.AppCode;
using System.Data;

namespace SendSMSForMC
{
    class classOutstandingBalancePaymentAuto
    {
        public static bool exitThread = false;
        public static string _updateDateTime = null;
        private static classDataAccess _dataAccess = new classDataAccess();
        public static string SMS_TYPE = "AUTPAY";

        public static void RunService()
        {
            int day = 0;
            int hour = 0;
            int minute = 0;
            int valueDay1 = 0;
            int valueDay2 = 0;
            int valueDay3 = 0;
            int valueBeginHour = 0;
            int valueEndHour = 0;
            DataTable table = new DataTable();

            valueDay1 = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Auto_Day_1");
            valueDay2 = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Auto_Day_2");
            valueDay3 = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Auto_Day_3");
            valueBeginHour = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Auto_Begin_Hour");
            valueEndHour = classUtilities.GetIntValueFromConfig("Payment_Outstanding_Auto_End_Hour");

            while (exitThread == false)
            {
                day = DateTime.Now.Day;
                hour = DateTime.Now.Hour;
                minute = DateTime.Now.Minute;

                //if ((day == valueDay1 || day == valueDay2 || day == valueDay3) && (hour >= valueBeginHour && hour <= valueEndHour))
                if(minute % 1 == 0) // 5 phut moi chay 1 lan
                {
                    try
                    {
                        classOutstandingBalancePaymentAutoLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        //table = Get_OutBal_Payment_Auto();
                        table = Get_BIN_ACQ();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classOutstandingBalancePaymentAutoLogWriter.WriteLog("----------------End Process-----------------");
                        //Thread.Sleep(1000 * 10);// sleep 1 phut de troi qua thoi gian lap lai
                    }
                    catch (Exception ex)
                    {
                        classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }
                Thread.Sleep(1000 * 50);// * 60);
            }
            
        }
        
        private static DataTable Get_BIN_ACQ()
        {
           
            DataTable table = new DataTable();         
            table = _dataAccess.GetBinACQ();
           
            return table;
        }
        private static DataTable Get_OutBal_Payment_Auto()
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

                //DataTable updateTime = _dataAccess.Get_Max_OutBal_UpdateTime(SMS_TYPE);
                //if (updateTime.Rows.Count == 1)
                //    maxUpdateDT = updateTime.Rows[0].ItemArray[0].ToString();
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error Get_OutBal_Payment_Auto(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetPaymentAuto(maxUpdateDT);
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
            int count = 0;

            foreach (DataRow row in table.Rows)
            {
                result = 0;
                message = CreateSMSMessage(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                                                    row.ItemArray[6].ToString(), row.ItemArray[7].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    if (row.ItemArray[1].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                                        , row.ItemArray[1].ToString()//classDataAccess.MYPHONE
                                                                        , message
                                                                        , 'Y'//Sent (se ko gui tin nhan)
                                                                        , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                                        , row.ItemArray[1].ToString()//classDataAccess.MYPHONE
                                                                        , message
                                                                        , 'N'//Sent (se ko gui tin nhan)
                                                                        , SMS_TYPE);
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[1].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertPaymnetAutoSMSToDW(SMS_TYPE, message
                                                , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                                , row.ItemArray[2].ToString()
                                                , row.ItemArray[4].ToString()
                                                , row.ItemArray[3].ToString()
                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                , row.ItemArray[8].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                                , row.ItemArray[9].ToString()
                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertPaymnetAutoSMSToDW(SMS_TYPE, message
                                               , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                               , DateTime.Parse(row.ItemArray[0].ToString())
                                               , row.ItemArray[2].ToString()
                                               , row.ItemArray[4].ToString()
                                               , row.ItemArray[3].ToString()
                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                               , row.ItemArray[8].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                               , double.Parse(row.ItemArray[7].ToString())//    , int.Parse(row.ItemArray[8].ToString())
                                               , row.ItemArray[9].ToString()
                                               , "N"
                                                               );
                        }
                    }
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classOutstandingBalancePaymentAutoLogWriter.WriteLog("So luong message da duoc Insert vao EbankGW thanh cong: " + count);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string date, string time, string amount)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.PadLeft(6, '0'); // vi co truong hop chi co 5 ky tu
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                //string crncyAlpha = ConvertCrncyCodeToCrncyAlpha(crncyCode);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                string smsMessage = "THE " + brand + " x" + pan + " " + date + " " + time
                                + " THANH TOAN " + amount1 + "VND " + "(THANH TOAN TU DONG DU NO THE MASTERCARD)";
                if (smsMessage.Length > 160)
                    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
