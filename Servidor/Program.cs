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
        // Mutex para garantir o acesso sequencial à escrita de ficheiros (Aula 3)
        private static Mutex mutex = new Mutex();

        static void Main(string[] args)
        {
            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("=== SERVIDOR INICIADO ===");
            Console.WriteLine("A escutar na porta 9000 por conexões de Gateways...");

            while (true)
            {
                // Aceita as conexões e cria uma Thread para lidar com múltiplos Gateways em simultâneo
                TcpClient client = listener.AcceptTcpClient();
                Console.WriteLine($"\n[Nova Conexão] Gateway conectado: {client.Client.RemoteEndPoint}");

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
                    if (bytesRead == 0) break; // O Gateway desconectou-se

                    string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[Recebido] {message}");

                    // Protocolo esperado: DATA|SENSOR_ID|TIPO_DADO|VALOR|...
                    string[] parts = message.Split('|');

                    // PROTEÇÃO APLICADA: Garantir que existem pelo menos 3 partes antes de ler o parts[2]
                    if (parts.Length >= 3 && parts[0] == "DATA")
                    {
                        string tipoDado = parts[2]; // Ex: TEMP, RUIDO, PM2.5
                        GuardarDados(tipoDado, message);
                    }
                    else if (parts[0] == "DATA" && parts.Length < 3)
                    {
                        // Se diz que é DATA mas não tem as partes todas, avisa na consola
                        Console.WriteLine("[AVISO] Mensagem ignorada por formato inválido ou incompleta.");
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
            // Protege o acesso ao ficheiro de texto usando o Mutex
            mutex.WaitOne();
            try
            {
                string fileName = $"{tipoDado}.txt";
                // AppendAllText cria o ficheiro se não existir, ou adiciona no fim se já existir
                File.AppendAllText(fileName, linhaData + Environment.NewLine);
            }
            finally
            {
                // O bloco finally garante que o Mutex é libertado, mesmo que a escrita no ficheiro dê erro
                mutex.ReleaseMutex();
            }
        }
    }
}