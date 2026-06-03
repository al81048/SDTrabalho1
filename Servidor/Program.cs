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
        static Mutex dbMutex = new Mutex(); // Protege a base de dados contra escritas/leituras simultâneas

        static void Main(string[] args)
        {
            Console.WriteLine("=== SERVIDOR CENTRAL ===");
            InicializarBaseDados();

            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("[+] Servidor à escuta na porta 9000...");

            // Thread separada para continuar a aceitar mensagens da Gateway em pano de fundo
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

            // ====================================================================
            // FASE 3: INTERFACE DE VISUALIZAÇÃO E EXPLORAÇÃO DE DADOS (CLI)
            // ====================================================================
            // Pequeno atraso só para o menu não se misturar com as mensagens de arranque
            Thread.Sleep(500);

            while (true)
            {
                Console.WriteLine("\n=== PAINEL DE ADMINISTRAÇÃO (Consultas) ===");
                Console.WriteLine("1. Ver Todas as Medições (Últimas 20)");
                Console.WriteLine("2. Pesquisar por ID do Sensor (ex: S101)");
                Console.WriteLine("3. Pesquisar por Tipo de Dado (ex: TEMP)");
                Console.WriteLine("0. Sair");
                Console.Write("Opção: ");

                string opcao = Console.ReadLine();

                if (opcao == "1")
                {
                    ConsultarBaseDados("SELECT * FROM Medicoes ORDER BY DataHora DESC LIMIT 20");
                }
                else if (opcao == "2")
                {
                    Console.Write("Introduza o ID do Sensor: ");
                    string id = Console.ReadLine();
                    ConsultarBaseDados($"SELECT * FROM Medicoes WHERE SensorId = '{id}' ORDER BY DataHora DESC LIMIT 20");
                }
                else if (opcao == "3")
                {
                    Console.Write("Introduza o Tipo de Dado: ");
                    string tipo = Console.ReadLine();
                    ConsultarBaseDados($"SELECT * FROM Medicoes WHERE Tipo = '{tipo}' ORDER BY DataHora DESC LIMIT 20");
                }
                else if (opcao == "0")
                {
                    Console.WriteLine("A encerrar o Servidor Central...");
                    Environment.Exit(0);
                }
                else
                {
                    Console.WriteLine("Opção inválida.");
                }
            }
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

                        // Re-imprime a indicação do menu para o utilizador não se perder
                        Console.Write("\nPressione ENTER para voltar ao menu ou introduza uma opção: ");
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

        // ====================================================================
        // FUNÇÃO DE LEITURA DA FASE 3 (Apresenta os dados em formato de Tabela)
        // ====================================================================
        static void ConsultarBaseDados(string query)
        {
            dbMutex.WaitOne();
            try
            {
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();
                    using (var cmd = new SQLiteCommand(query, conn))
                    using (var reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("\n------------------------------------------------------------------------------------------");
                        Console.WriteLine($"{"ID",-5} | {"DATA/HORA",-19} | {"SENSOR",-6} | {"TIPO",-5} | {"VALOR",-6} | {"ANÁLISE"}");
                        Console.WriteLine("------------------------------------------------------------------------------------------");

                        bool encontrouDados = false;
                        while (reader.Read())
                        {
                            encontrouDados = true;
                            Console.WriteLine($"{reader["Id"],-5} | {reader["DataHora"],-19} | {reader["SensorId"],-6} | {reader["Tipo"],-5} | {reader["Valor"],-6} | {reader["Analise"]}");
                        }

                        if (!encontrouDados)
                        {
                            Console.WriteLine("Nenhum registo encontrado na base de dados com estes critérios.");
                        }
                        Console.WriteLine("------------------------------------------------------------------------------------------\n");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Erro na Consulta]: {ex.Message}");
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