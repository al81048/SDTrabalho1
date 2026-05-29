using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;

namespace ServidorCentral
{
    class Program
    {
        private static string ligacaoBD = "Data Source=central.db;";
        private static Mutex dbMutex = new Mutex();

        static void Main(string[] args)
        {
            Console.WriteLine("=== SERVIDOR CENTRAL ===");

            // Verifica as tabelas e introduz os sensores se a BD estiver vazia 
            PrepararBD();

            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            listener.Start();
            Console.WriteLine("[+] Servidor à escuta na porta 9000...");

            while (true)
            {
                TcpClient gatewayClient = listener.AcceptTcpClient();
                Thread t = new Thread(() => HandleGateway(gatewayClient));
                t.Start();
            }
        }

        static void HandleGateway(TcpClient client)
        {
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[1024];

            try
            {
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead > 0)
                {
                    string msg = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"\n[Recebido do Gateway]: {msg}");

                    ProcessarMensagem(msg);
                }
            }
            catch (Exception ex) { Console.WriteLine($"Erro na receção: {ex.Message}"); }
            finally { client.Close(); }
        }

        static void ProcessarMensagem(string msg)
        {
            string[] parts = msg.Split('|');
            if (parts.Length < 2) return;

            string comando = parts[0];
            string sensorId = parts[1];

            // necessário este mutex porque o sqlite não é thread-safe e pode haver múltiplas Threads a tentar aceder à BD ao mesmo tempo
            dbMutex.WaitOne();
            try
            {
                using (var conn = new SqliteConnection(ligacaoBD))
                {
                    conn.Open();

                    string sqlCheck = "SELECT estado FROM Sensores WHERE id = @id";
                    using (var cmd = new SqliteCommand(sqlCheck, conn))
                    {
                        cmd.Parameters.AddWithValue("@id", sensorId);
                        object estado = cmd.ExecuteScalar();

                        // O sensor existe E está ativo?
                        if (estado != null && estado.ToString() == "ativo")
                        {
                            // Regista o último contacto (PING, HELLO ou DATA)
                            string sqlUpdate = "UPDATE Sensores SET last_sync = @hora WHERE id = @id";
                            using (var cmdUpd = new SqliteCommand(sqlUpdate, conn))
                            {
                                cmdUpd.Parameters.AddWithValue("@hora", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                                cmdUpd.Parameters.AddWithValue("@id", sensorId);
                                cmdUpd.ExecuteNonQuery();
                            }

                            // Se for envio de dados, guarda na tabela de histórico
                            if (comando == "DATA" && parts.Length >= 4)
                            {
                                string tipoDado = parts[2];
                                string valorDado = parts[3];

                                string sqlInsert = "INSERT INTO Medicoes (sensor_id, tipo, valor, data_hora) VALUES (@id, @tipo, @val, @data)";
                                using (var cmdInsert = new SqliteCommand(sqlInsert, conn))
                                {
                                    cmdInsert.Parameters.AddWithValue("@id", sensorId);
                                    cmdInsert.Parameters.AddWithValue("@tipo", tipoDado);
                                    cmdInsert.Parameters.AddWithValue("@val", valorDado);
                                    cmdInsert.Parameters.AddWithValue("@data", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                                    cmdInsert.ExecuteNonQuery();
                                    Console.WriteLine($"[BD] Medição de {sensorId} guardada com sucesso.");
                                }

                                // ---------------------------------------------------------
                                // FASE 1 TP2: CHAMADA RPC AO SERVIÇO PYTHON
                                // ---------------------------------------------------------
                                string analise = ChamarServicoAnaliseRPC(tipoDado, valorDado);
                                Console.WriteLine($"[ANÁLISE EXTERNA] Risco para a Saúde: {analise}");
                                // ---------------------------------------------------------
                            }
                        }
                        else if (estado != null)
                        {
                            // Se existir mas estiver inativo ou em manutenção
                            Console.WriteLine($"[AVISO] Dados ignorados. O Sensor {sensorId} está no estado: '{estado}'.");
                        }
                        else
                        {
                            // Se não existir de todo na BD
                            Console.WriteLine($"[AVISO] Sensor {sensorId} bloqueado (Intruso/Inexistente).");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO BD] Falha ao processar mensagem: {ex.Message}");
            }
            finally
            {
                dbMutex.ReleaseMutex();
            }
        }

        // =================================================================================
        // NOVA FUNÇÃO: Cliente RPC para comunicar com o Python (Porta 8000)
        // =================================================================================
        static string ChamarServicoAnaliseRPC(string tipo, string valor)
        {
            try
            {
                // Liga-se ao Serviço Python na porta 8000 (localhost)
                using (TcpClient rpcClient = new TcpClient("127.0.0.1", 8000))
                using (NetworkStream stream = rpcClient.GetStream())
                {
                    // Formata a mensagem no protocolo RPC que definimos no Python: PREVER|TIPO|VALOR
                    string msgRPC = $"PREVER|{tipo}|{valor}";
                    byte[] dataOut = Encoding.UTF8.GetBytes(msgRPC);

                    // Envia o pedido
                    stream.Write(dataOut, 0, dataOut.Length);

                    // Fica à espera da resposta do Python
                    byte[] dataIn = new byte[1024];
                    int bytesLidos = stream.Read(dataIn, 0, dataIn.Length);

                    // Converte os bytes recebidos de volta para texto
                    return Encoding.UTF8.GetString(dataIn, 0, bytesLidos);
                }
            }
            catch
            {
                // Se o script Python no Spyder estiver desligado, o C# não vai "crashar". 
                // Apenas devolve este aviso amigável.
                return "[ERRO RPC] Serviço de Análise Python offline. Ligue o script no Spyder.";
            }
        }
        // =================================================================================

        static void PrepararBD()
        {
            try
            {
                using (var conn = new SqliteConnection(ligacaoBD))
                {
                    conn.Open();

                    // Tabela de Sensores AUMENTADA para guardar a Zona e os Tipos
                    string tableSensores = "CREATE TABLE IF NOT EXISTS Sensores (id VARCHAR(20) PRIMARY KEY, estado VARCHAR(20), zona VARCHAR(50), tipos VARCHAR(50), last_sync VARCHAR(30));";
                    string tableMedicoes = "CREATE TABLE IF NOT EXISTS Medicoes (id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id VARCHAR(20), tipo VARCHAR(20), valor VARCHAR(20), data_hora VARCHAR(30));";

                    using (var cmd = new SqliteCommand(tableSensores, conn)) cmd.ExecuteNonQuery();
                    using (var cmd = new SqliteCommand(tableMedicoes, conn)) cmd.ExecuteNonQuery();

                    // Verifica se a tabela está vazia
                    string checkEmpty = "SELECT COUNT(*) FROM Sensores";
                    using (var cmdCheck = new SqliteCommand(checkEmpty, conn))
                    {
                        long count = (long)cmdCheck.ExecuteScalar();
                        if (count == 0)
                        {
                            // Colocar alguns sensores na base de dados para testes
                            string insertData = "INSERT INTO Sensores (id, estado, zona, tipos, last_sync) VALUES " +
                                                "('S100', 'ativo', 'ZONA CENTRO', '[TEMP]', ''), " +
                                                "('S101', 'ativo', 'ZONA ESCOLAR', '[TEMP,RUIDO]', ''), " +
                                                "('S102', 'inativo', 'ZONA SUL', '[PM2.5]', ''), " +
                                                "('S103', 'manutencao', 'ZONA NORTE', '[HUM]', '')";

                            using (var cmdInsert = new SqliteCommand(insertData, conn))
                            {
                                cmdInsert.ExecuteNonQuery();
                            }
                            Console.WriteLine("[BD] Os Sensores foram carregados com sucesso.");
                        }
                    }
                }
                Console.WriteLine("[BD] Base de dados central verificada/pronta a usar.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO AO PREPARAR BD] {ex.Message}");
            }
        }
    }
}