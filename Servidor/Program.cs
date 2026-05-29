using System;
using System.Data.SQLite; // Necessário pacote NuGet: System.Data.SQLite
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace ServidorCentralApp
{
    class Program
    {
        static string connectionString = "Data Source=onehealth.db;Version=3;";
        static Mutex dbMutex = new Mutex(); // Protege a base de dados contra escritas simultâneas

        static void Main(string[] args)
        {
            Console.WriteLine("=== SERVIDOR CENTRAL ===");
            InicializarBaseDados();

            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("[+] Servidor à escuta na porta 9000...");

            // Thread separada para continuar a aceitar mensagens da Gateway
            Thread serverThread = new Thread(() =>
            {
                while (true)
                {
                    TcpClient client = listener.AcceptTcpClient();
                    Thread clientThread = new Thread(() => ProcessarCliente(client));
                    clientThread.Start();
                }
            });
            serverThread.Start();

            // O programa principal fica bloqueado aqui (Preparado para o Menu da Fase 3)
            // Na próxima fase vamos colocar aqui o menu de interatividade!
        }

        static void ProcessarCliente(TcpClient client)
        {
            using (NetworkStream stream = client.GetStream())
            {
                byte[] buffer = new byte[1024];
                int bytesRead = stream.Read(buffer, 0, buffer.Length);

                if (bytesRead > 0)
                {
                    string msgRecebida = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"\n[Recebido do Gateway]: {msgRecebida}");

                    string[] parts = msgRecebida.Split('|');

                    if (parts.Length >= 4 && parts[0] == "DATA")
                    {
                        string sensorId = parts[1];
                        string tipo = parts[2];
                        string valor = parts[3];

                        // 1. Chama Python (Porta 8000) para Análise de Risco
                        string analise = ChamarAnaliseRPC(tipo, valor);

                        // 2. Guarda na Base de Dados SQLite
                        GuardarMedicaoBD(sensorId, tipo, valor, analise);

                        Console.WriteLine($"[ANÁLISE EXTERNA] Risco para a Saúde: {analise}");
                    }
                }
            }
            client.Close();
        }

        static void InicializarBaseDados()
        {
            try
            {
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();
                    string query = @"CREATE TABLE IF NOT EXISTS Medicoes (
                                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        SensorId TEXT,
                                        Tipo TEXT,
                                        Valor TEXT,
                                        Analise TEXT,
                                        DataHora DATETIME DEFAULT CURRENT_TIMESTAMP
                                    )";
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("[BD] Base de dados central verificada/pronta a usar.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Erro BD]: {ex.Message}");
            }
        }

        static void GuardarMedicaoBD(string sensorId, string tipo, string valor, string analise)
        {
            dbMutex.WaitOne();
            try
            {
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();
                    string query = "INSERT INTO Medicoes (SensorId, Tipo, Valor, Analise) VALUES (@sid, @tipo, @val, @analise)";
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@sid", sensorId);
                        cmd.Parameters.AddWithValue("@tipo", tipo);
                        cmd.Parameters.AddWithValue("@val", valor);
                        cmd.Parameters.AddWithValue("@analise", analise);
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine($"[BD] Medição de {sensorId} guardada com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Erro ao Guardar]: {ex.Message}");
            }
            finally
            {
                dbMutex.ReleaseMutex();
            }
        }

        static string ChamarAnaliseRPC(string tipo, string valor)
        {
            try
            {
                using (TcpClient rpcClient = new TcpClient("127.0.0.1", 8000))
                using (NetworkStream stream = rpcClient.GetStream())
                {
                    string msgRPC = $"PREVER|{tipo}|{valor}";
                    byte[] dataOut = Encoding.UTF8.GetBytes(msgRPC);
                    stream.Write(dataOut, 0, dataOut.Length);

                    byte[] dataIn = new byte[1024];
                    int bytesLidos = stream.Read(dataIn, 0, dataIn.Length);
                    return Encoding.UTF8.GetString(dataIn, 0, bytesLidos);
                }
            }
            catch
            {
                return "Erro de ligação ao módulo de análise (Python).";
            }
        }
    }
}