//gateway quando se implementa a base de dados
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace GatewayApp
{
    class Program
    {
        private static string serverIP = "127.0.0.1";
        private static int serverPort = 9000;

        static void Main(string[] args)
        {
            Console.WriteLine("=== GATEWAY ONE HEALTH (PROXY) ===");

            // Inicia o servidor para ouvir Sensores na porta 5000
            TcpListener listener = new TcpListener(IPAddress.Any, 5000);
            listener.Start();
            Console.WriteLine("[+] Gateway à escuta de Sensores na porta 5000...");

            while (true)
            {
                // Aceita nova conexão de um Sensor e cria uma Thread
                TcpClient sensorClient = listener.AcceptTcpClient();
                Console.WriteLine($"\n[Nova Conexão] Sensor detetado: {sensorClient.Client.RemoteEndPoint}");

                Thread t = new Thread(() => HandleSensor(sensorClient));
                t.Start();
            }
        }

        static void HandleSensor(TcpClient sensorClient)
        {
            NetworkStream sensorStream = sensorClient.GetStream();
            byte[] buffer = new byte[1024];

            try
            {
                while (true)
                {
                    int bytesRead = sensorStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break; // Sensor desligou-se

                    string msg = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[Sensor -> Gateway]: {msg}");

                    // A MAGIA DO PROXY: Já não há validação por CSV aqui!
                    // Tudo (HELLO, DATA, PING, VIDEO) é imediatamente enviado ao Servidor.
                    EncaminharParaServidor(msg);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro na ligação com o Sensor: {ex.Message}");
            }
            finally
            {
                sensorClient.Close();
                Console.WriteLine("[Desconexão] Um sensor desligou-se.");
            }
        }

        static void EncaminharParaServidor(string mensagem)
        {
            try
            {
                // Abre ligação rápida ao Servidor Central (Porta 9000)
                using (TcpClient serverClient = new TcpClient(serverIP, serverPort))
                using (NetworkStream serverStream = serverClient.GetStream())
                {
                    byte[] data = Encoding.UTF8.GetBytes(mensagem);
                    serverStream.Write(data, 0, data.Length);
                    Console.WriteLine("[Gateway -> Servidor]: Encaminhado com sucesso.");
                }
            }
            catch
            {
                Console.WriteLine("[ERRO] Não foi possível ligar ao Servidor Principal na porta 9000. O Servidor está a correr?");
            }
        }
    }
}
