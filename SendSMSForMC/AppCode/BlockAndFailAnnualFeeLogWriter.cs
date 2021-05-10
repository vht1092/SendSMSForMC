using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Windows.Forms;

namespace SendSMSForMC
{
    class classBlockAndFailAnnualFeeLogWriter
    {
        public static FileStream fs;
        public static StreamWriter w;

        public classBlockAndFailAnnualFeeLogWriter()
        {
            fs = null;
            w = null;
        }

        public static void OpenFileWriter()
        {
            string rootpath = Application.StartupPath;
            string filename = rootpath + "\\log\\" + "BlockAndFailAnnualFee_" + DateTime.Today.ToString("yyyyMMdd") + ".log";
            try
            {
                fs = new FileStream(filename, FileMode.Append);
                w = new StreamWriter(fs, Encoding.ASCII);
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message);
                return;
            }
        }

        public static void CloseFileWriter()
        {
            if (w != null)
            {
                w.Close();
            }
            if (fs != null)
            {
                fs.Close();
            }
        }

        public static void WriteLog(string content)
        {
            OpenFileWriter();
            content = DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss ") + content;
            w.WriteLine(content);
            w.Flush();
            CloseFileWriter();
        }
    }
}
