using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Collections.Generic;

namespace GatewayApp
{
    class Program
    {
        // Mutex para proteger a leitura e escrita no ficheiro config.csv (Aula 3)
        private static Mutex csvMutex = new Mutex();

        // Caminho do ficheiro (Lembra-te da nota importante abaixo sobre a entrega!)
        private static string csvPath = @"C:\Users\Utilizador\Desktop\config.csv";

        private static string serverIP = "127.0.0.1";
        private static int serverPort = 9000;

        static void Main(string[] args)
        {
            Console.WriteLine("=== GATEWAY ONE HEALTH ===");

            // Inicia o servidor para ouvir Sensores na porta 5000 (Aula 2)
            TcpListener listener = new TcpListener(IPAddress.Any, 5000);
            listener.Start();
            Console.WriteLine("[+] Gateway à escuta de Sensores na porta 5000...");

            while (true)
            {
                // Aceita nova conexão de um Sensor e cria uma Thread para não bloquear (Aula 3)
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
                    if (bytesRead == 0) break;

                    string msg = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[Sensor -> Gateway]: {msg}");

                    // Protocolo: TIPO|ID|RESTO...
                    string[] parts = msg.Split('|');

                    // Proteção: Só processa se a mensagem tiver pelo menos TIPO e ID
                    if (parts.Length >= 2)
                    {
                        string comando = parts[0];
                        string sensorId = parts[1];

                        // 1. Validar Sensor no ficheiro CSV
                        if (ValidarEAtualizarSensor(sensorId))
                        {
                            // 2. Se for uma medição (DATA), encaminha para o Servidor Principal
                            if (comando == "DATA")
                            {
                                EncaminharParaServidor(msg);
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("[AVISO] Mensagem ignorada (formato inválido).");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro na ligação com o Sensor: {ex.Message}");
            }
            finally
            {
                sensorClient.Close();
            }
        }

        static bool ValidarEAtualizarSensor(string id)
        {
            bool sensorValido = false;

            // Bloqueia o acesso ao ficheiro para outras Threads (Aula 3)
            csvMutex.WaitOne();
            try
            {
                if (!File.Exists(csvPath))
                {
                    Console.WriteLine($"[ERRO CRÍTICO] O ficheiro {csvPath} não foi encontrado na pasta!");
                    return false;
                }

                string[] linhas = File.ReadAllLines(csvPath);
                List<string> novasLinhas = new List<string>();

                foreach (string linha in linhas)
                {
                    // A NOSSA GRANDE ALTERAÇÃO: O limite de 5 partes salva a data de ser cortada!
                    string[] campos = linha.Split(':', 5);

                    if (campos.Length >= 5 && campos[0] == id)
                    {
                        if (campos[1] == "ativo") // O sensor existe e está ativo
                        {
                            sensorValido = true;
                            // Atualiza a data do último heartbeat (last_sync)
                            campos[4] = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                        }
                        // Reconstrói a linha com a data atualizada
                        novasLinhas.Add(string.Join(":", campos));
                    }
                    else
                    {
                        novasLinhas.Add(linha);
                    }
                }

                // Se o sensor for válido, reescreve o ficheiro todo com a nova data
                if (sensorValido)
                {
                    File.WriteAllLines(csvPath, novasLinhas);
                }
                else
                {
                    Console.WriteLine($"[AVISO] Sensor {id} rejeitado (Inexistente ou Inativo no CSV).");
                }
            }
            finally
            {
                // Liberta sempre o ficheiro, mesmo que dê erro pelo meio
                csvMutex.ReleaseMutex();
            }

            return sensorValido;
        }

        static void EncaminharParaServidor(string mensagem)
        {
            try
            {
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
                Console.WriteLine("[ERRO] Não foi possível ligar ao Servidor Principal na porta 9000.");
            }
        }
    }
}