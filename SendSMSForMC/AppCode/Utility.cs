using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SendSMSForMC.AppCode
{
    class Utility
    {
        /**
       <summary>
       Converts a hexadecimal string to a byte array.
       </summary>
       <param name="s"></param>
       <returns></returns>
       <remarks></remarks>
        * */
        public static byte[] HexToByteArray(string hex)
        {
            return Enumerable.Range(0, hex.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(hex.Substring(x, 2), 16))
                             .ToArray();
        }

        /**
        <summary>
        Converts a byte array to a hexadecimal string.
        </summary>
        <param name="bData"></param>
        <returns></returns>
        <remarks></remarks>
         * */
        public static string ByteArrayToHex(byte[] bData)
        {
            StringBuilder hex = new StringBuilder(bData.Length * 2);
            foreach (byte b in bData)
                hex.AppendFormat("{0:x2}", b);
            return hex.ToString();


        }
    }
}
