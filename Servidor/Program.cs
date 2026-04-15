using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Servidor
{
    class Program
    {
        // 1. Substituímos o 'object fileLock' pelo Mutex da Aula 3
        private static Mutex mutex = new Mutex();

        static void Main(string[] args)
        {
            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("=== SERVIDOR INICIADO ===");
            Console.WriteLine("A escutar na porta 9000 por conexões de Gateways...");

            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                Console.WriteLine($"[Nova Conexão] Gateway conectado: {client.Client.RemoteEndPoint}");

                Thread gatewayThread = new Thread(() => HandleGateway(client));
                gatewayThread.Start();
            }
        }

        static void HandleGateway(TcpClient client)
        {
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[1024];

            try
            {
                while (true)
                {
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[Recebido] {message}");

                    // Protocolo: DATA|SENSOR_ID|TIPO_DADO|VALOR|TIMESTAMP
                    string[] parts = message.Split('|');

                    if (parts.Length > 0 && parts[0] == "DATA")
                    {
                        string tipoDado = parts[2];
                        GuardarDados(tipoDado, message);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Erro de Conexão] {ex.Message}");
            }
            finally
            {
                client.Close();
                Console.WriteLine("[Desconexão] Um Gateway desligou-se.");
            }
        }

        static void GuardarDados(string tipoDado, string linhaData)
        {
            // 2. Usamos o WaitOne e ReleaseMutex em vez do 'lock'
            mutex.WaitOne();
            try
            {
                string fileName = $"{tipoDado}.txt";
                File.AppendAllText(fileName, linhaData + Environment.NewLine);
            }
            finally
            {
                // O bloco finally garante que o Mutex é libertado mesmo que dê erro ao escrever no ficheiro
                mutex.ReleaseMutex();
            }
        }
    }
}