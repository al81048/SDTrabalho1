/*using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace GatewayApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== GATEWAY BÁSICO (A PONTE) ===");

            try
            {
                // ==========================================
                // PASSO 1: OUVIR O SENSOR (PORTA 5000)
                // ==========================================
                TcpListener listener = new TcpListener(IPAddress.Parse("127.0.0.1"), 5000);
                listener.Start();
                Console.WriteLine("[1] À escuta de Sensores na porta 5000...");

                // O Gateway fica parado aqui até o Sensor se ligar
                TcpClient sensorClient = listener.AcceptTcpClient();
                Console.WriteLine("[2] Um Sensor conectou-se!");

                // Lê a mensagem enviada pelo Sensor
                NetworkStream sensorStream = sensorClient.GetStream();
                byte[] buffer = new byte[1024];
                int bytesRead = sensorStream.Read(buffer, 0, buffer.Length);
                string mensagemDoSensor = Encoding.UTF8.GetString(buffer, 0, bytesRead);

                Console.WriteLine($"[3] Mensagem recebida: {mensagemDoSensor}");

                // Já temos a mensagem, podemos fechar a ligação com o Sensor
                sensorClient.Close();
                listener.Stop();


                // ==========================================
                // PASSO 2: ENVIAR PARA O SERVIDOR (PORTA 9000)
                // ==========================================
                Console.WriteLine("\n[4] A reencaminhar a mensagem para o Servidor Principal...");

                // Conecta-se ao Servidor (que tem de estar a correr primeiro)
                TcpClient servidorClient = new TcpClient("127.0.0.1", 9000);
                NetworkStream servidorStream = servidorClient.GetStream();

                // Converte a mensagem recebida de volta para bytes e envia
                byte[] dataParaServidor = Encoding.UTF8.GetBytes(mensagemDoSensor);
                servidorStream.Write(dataParaServidor, 0, dataParaServidor.Length);

                Console.WriteLine("[5] Mensagem entregue ao Servidor com sucesso!");

                // Fecha a ligação ao servidor
                servidorClient.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[ERRO]: {ex.Message}");
                Console.WriteLine("Dica: Certifica-te de que o Servidor Principal já está a correr na porta 9000!");
            }

            Console.WriteLine("\nGateway finalizado. Pressiona Enter para sair.");
            Console.ReadLine();
        }
    }
}*/

//gateway desenvolvido
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
        // Mutex para proteger a leitura/escrita no ficheiro config.csv (Aula 3)
        private static Mutex csvMutex = new Mutex();
        private static string csvPath = "config.csv";
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
                // Aceita nova conexão de um Sensor
                TcpClient sensorClient = listener.AcceptTcpClient();
                Console.WriteLine($"\n[Nova Conexão] Sensor detetado: {sensorClient.Client.RemoteEndPoint}");

                // Cria uma Thread para este sensor (Aula 3)
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
                    string comando = parts[0];
                    string sensorId = parts[1];

                    // 1. Validar Sensor no CSV (Fase 3 do Protocolo)
                    if (ValidarEAtualizarSensor(sensorId, parts))
                    {
                        // 2. Se for DATA, encaminhar para o Servidor (Fase 2)
                        if (comando == "DATA")
                        {
                            EncaminharParaServidor(msg);
                        }
                    }
                }
            }
            catch (Exception ex) { Console.WriteLine($"Erro: {ex.Message}"); }
            finally { sensorClient.Close(); }
        }

        static bool ValidarEAtualizarSensor(string id, string[] msgParts)
        {
            bool sensorValido = false;
            csvMutex.WaitOne(); // Protege o ficheiro (Aula 3)
            try
            {
                string[] linhas = File.ReadAllLines(csvPath);
                List<string> novasLinhas = new List<string>();

                foreach (string linha in linhas)
                {
                    // Formato CSV: sensor_id:estado:zona:[tipos]:last_sync
                    string[] campos = linha.Split(':');
                    if (campos[0] == id)
                    {
                        if (campos[1] == "ativo") // Verifica estado (Fase 3)
                        {
                            sensorValido = true;
                            // Atualiza o last_sync para o tempo atual
                            campos[4] = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                        }
                        novasLinhas.Add(string.Join(":", campos));
                    }
                    else { novasLinhas.Add(linha); }
                }

                if (sensorValido) File.WriteAllLines(csvPath, novasLinhas);
                else Console.WriteLine($"[AVISO] Sensor {id} rejeitado (Inexistente ou Inativo).");
            }
            finally { csvMutex.ReleaseMutex(); }
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
            catch { Console.WriteLine("[ERRO] Não foi possível ligar ao Servidor Principal."); }
        }
    }
}