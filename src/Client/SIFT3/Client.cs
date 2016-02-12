using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.IO;


namespace SIFT3
{
	class Client
	{
		Socket client;

		public Client(string host, int port)
		{
			IPAddress ipAddr = IPAddress.Parse(host);
			IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, port);

			client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
			client.Connect(ipEndPoint);
		}

		public void SendString(string str)
		{
			byte[] buffer = Encoding.UTF8.GetBytes(str);
			client.Send(buffer);
		}

		public void SendFile(string filePath)
		{
			FileInfo fileInfo = new FileInfo(filePath);
			string header = "PUT\n" +
				string.Format("File: filePath=\"{0}\"; length={1}\n\n", fileInfo.Name, fileInfo.Length);
			SendString(header);
			client.SendFile(filePath);
		}
		public String Receive()
		{
			Byte[] bytesReceived = new Byte[256];
			string ans = "";
			int bytes = 0;
			do{
				bytes = client.Receive(bytesReceived, bytesReceived.Length, 0);
				ans = ans + Encoding.ASCII.GetString(bytesReceived, 0, bytes);
				if (ans.Length > 0 && ans[ans.Length - 1] == '\n') break;
			} while (bytes > 0);

			return ans.Replace("\n", "") ;
		}

		public void Close()
		{
			client.Shutdown(SocketShutdown.Both);
			client.Close();
		}
	}
}
