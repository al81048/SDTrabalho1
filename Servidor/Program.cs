/*/
//servidor utilizando csv
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
}*/

// servidor utilizando base de dados
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
                    Console.WriteLine($"[Recebido do Gateway]: {msg}");

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
                                string sqlInsert = "INSERT INTO Medicoes (sensor_id, tipo, valor, data_hora) VALUES (@id, @tipo, @val, @data)";
                                using (var cmdInsert = new SqliteCommand(sqlInsert, conn))
                                {
                                    cmdInsert.Parameters.AddWithValue("@id", sensorId);
                                    cmdInsert.Parameters.AddWithValue("@tipo", parts[2]);
                                    cmdInsert.Parameters.AddWithValue("@val", parts[3]);
                                    cmdInsert.Parameters.AddWithValue("@data", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                                    cmdInsert.ExecuteNonQuery();
                                    Console.WriteLine($"[BD] Medição de {sensorId} guardada com sucesso.");
                                }
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