using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace StormNet
{
    public abstract class Spout
    {
        public abstract void Open(StormEnvironment environment);

        public abstract void NextTuple();

        public abstract void Ack(string id);

        public abstract void Fail(string id);

        public void Run()
        {
            string heartbeatDir = Protocol.ReadStringMessage();
            Protocol.SendProcessId(heartbeatDir);
            var env = Protocol.GetEnvironment();
            
            try
            {
                while (true)
                {
                    var msg = Protocol.ReadMessage();
                    switch (msg["command"])
                    {
                        case "next":
                            NextTuple();
                            break;
                        case "ack":
                            Ack(msg["id"]);
                            break;
                        case "fail":
                            Fail(msg["id"]);
                            break;
                    }
                    Protocol.Sync();
                }
            }
            catch (Exception ex)
            {
                Protocol.Log(ex.Message);
            }
        }
    }
}
