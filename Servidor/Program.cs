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
        // Este é o nosso "Mutex/Lock". Garante que apenas uma thread escreve no ficheiro de cada vez.
        private static readonly object fileLock = new object();

        static void Main(string[] args)
        {
            // O Servidor fica à escuta na porta 9000
            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("=== SERVIDOR INICIADO ===");
            Console.WriteLine("A escutar na porta 9000 por conexões de Gateways...");

            while (true)
            {
                // Fica bloqueado aqui até um Gateway se conectar
                TcpClient client = listener.AcceptTcpClient();
                Console.WriteLine($"[Nova Conexão] Gateway conectado: {client.Client.RemoteEndPoint}");

                // Cria uma Thread nova para atender este Gateway específico
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
                    // Lê a mensagem do Gateway
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break; // O Gateway desconectou-se

                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[Recebido] {message}");

                    // O nosso protocolo é: DATA|SENSOR_ID|TIPO_DADO|VALOR|TIMESTAMP
                    string[] parts = message.Split('|');

                    if (parts.Length > 0 && parts[0] == "DATA")
                    {
                        string tipoDado = parts[2]; // Ex: TEMP, RUIDO, PM2.5
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
            // O lock impede que duas threads tentem escrever no mesmo ficheiro ao mesmo tempo
            lock (fileLock)
            {
                string fileName = $"{tipoDado}.txt";
                // AppendAllText cria o ficheiro se não existir, ou adiciona no fim se já existir
                File.AppendAllText(fileName, linhaData + Environment.NewLine);
            }
        }
    }
}