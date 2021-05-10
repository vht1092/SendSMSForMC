using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SendSMSForMC.AppCode
{
    class HexKey
    {
        private string PartA;
        private string PartB;
        private string PartC;
        private KeyLength keyLength;

        public HexKey(string key)
        {
            if (key.Length == 16)
            {
                PartA = key;
                PartB = key;
                PartC = key;
                keyLength = KeyLength.SingleLength;
            }
            else if (key.Length == 32)
            {
                PartA = key.Substring(0, 16);
                PartB = key.Substring(16);
                PartC = PartA;
                keyLength = KeyLength.DoubleLength;
            }
            else if (key.Length == 48)
            {
                PartA = key.Substring(0, 16);
                PartB = key.Substring(16, 16);
                PartC = key.Substring(32);
                keyLength = KeyLength.TripleLength;
            }
        }

        public string toPrint()
        {
            string result = "";
            if (keyLength == KeyLength.SingleLength)
                result = PartA;
            else if (keyLength == KeyLength.DoubleLength)
                result = PartA + PartB;
            else if (keyLength == KeyLength.TripleLength)
                result = PartA + PartB + PartC;

            return result;
        }

        public string getPartA()
        {
            return PartA;
        }

        public string getPartB()
        {
            return PartB;
        }

        public string getPartC()
        {
            return PartC;
        }
    }
}
