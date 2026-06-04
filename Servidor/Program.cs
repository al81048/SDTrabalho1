// Importa as funcionalidades básicas do C# (como escrever na consola e aceder ao sistema operativo)
using System;
// Importa a biblioteca para criar e gerir a nossa base de dados local (requer o pacote NuGet)
using System.Data.SQLite;
// Importa funcionalidades para manipular ficheiros e descobrir caminhos de pastas no disco
using System.IO;
// Importa funcionalidades para trabalhar com endereços de rede (ex: endereços IP)
using System.Net;
// Importa as classes necessárias para criar ligações de rede TCP (Sockets e Listeners)
using System.Net.Sockets;
// Importa ferramentas para converter texto em bytes (e vice-versa), como a codificação UTF-8
using System.Text;
// Importa o sistema de Threads, permitindo que o programa faça várias coisas ao mesmo tempo
using System.Threading;

// Define o "espaço de nomes" para organizar o nosso projeto
namespace ServidorCentralApp
{
    // Classe principal onde todo o código vai correr
    class Program
    {
        // Descobre dinamicamente o caminho para o "Ambiente de Trabalho" do utilizador que estiver a correr o PC
        static string caminhoDesktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);

        // Junta o caminho do Ambiente de Trabalho ao nome do ficheiro da nossa base de dados
        static string caminhoBaseDados = Path.Combine(caminhoDesktop, "onehealth.db");

        // Cria a "Connection String" (a morada exata) que o C# vai usar para encontrar e abrir a base de dados
        static string connectionString = $"Data Source={caminhoBaseDados};Version=3;";

        // Cria um Mutex (Semáforo de Exclusão Mútua) para impedir que duas mensagens escrevam na BD ao mesmo tempo
        static Mutex dbMutex = new Mutex();

        // Função Main: É a primeira coisa que o programa executa quando arranca
        static void Main(string[] args)
        {
            // Escreve o título do programa na consola
            Console.WriteLine("=== SERVIDOR CENTRAL ===");

            // Chama a função que verifica se a base de dados existe (se não existir, cria-a)
            InicializarBaseDados();

            // Cria um "escutador" de rede que aceita ligações de qualquer IP na porta 9000
            TcpListener listener = new TcpListener(IPAddress.Any, 9000);
            // Liga o escutador (fica à espera que a Gateway se ligue)
            listener.Start();
            // Avisa o utilizador que o servidor já está pronto a receber dados
            Console.WriteLine("[+] Servidor à escuta na porta 9000...");

            // Cria uma Thread (um processo em segundo plano) separada SÓ para aceitar conexões da Gateway
            // Isto impede que o servidor bloqueie enquanto estamos a mexer no menu de opções
            Thread serverThread = new Thread(() =>
            {
                // Um ciclo infinito para o servidor nunca parar de escutar
                while (true)
                {
                    // Fica em pausa até a Gateway enviar uma mensagem. Quando envia, aceita a ligação
                    TcpClient client = listener.AcceptTcpClient();
                    // Cria UMA NOVA Thread separada só para processar a mensagem deste cliente específico
                    Thread clientThread = new Thread(() => ProcessarCliente(client));
                    // Inicia a thread que vai processar a mensagem
                    clientThread.Start();
                }
            });
            // Arranca a Thread principal do servidor (que fica a escutar no fundo)
            serverThread.Start();

            // Dá uma pausa de meio segundo só para as mensagens de arranque não se misturarem com o menu
            Thread.Sleep(500);

            // ====================================================================
            // FASE 3: INTERFACE DE VISUALIZAÇÃO E EXPLORAÇÃO DE DADOS (CLI)
            // ====================================================================

            // Ciclo infinito que mantém o menu do Painel de Administração sempre aberto
            while (true)
            {
                // Escreve as opções do menu no ecrã
                Console.WriteLine("\n=== PAINEL DE ADMINISTRAÇÃO (Consultas) ===");
                Console.WriteLine("1. Ver Todas as Medições (Últimas 20)");
                Console.WriteLine("2. Pesquisar por ID do Sensor (ex: S101)");
                Console.WriteLine("3. Pesquisar por Tipo de Dado (ex: TEMP)");
                Console.WriteLine("4. Pedir Nova Análise Manualmente (Simulador)");
                Console.WriteLine("0. Sair");
                Console.Write("Opção: ");

                // Lê o que o utilizador escreveu no teclado
                string opcao = Console.ReadLine();

                // Se escolheu 1, executa uma pesquisa (Query) à Base de Dados para mostrar os últimos 20 registos
                if (opcao == "1")
                {
                    ConsultarBaseDados("SELECT * FROM Medicoes ORDER BY DataHora DESC");
                }
                // Se escolheu 2, pergunta o ID e pesquisa apenas por esse Sensor
                else if (opcao == "2")
                {
                    Console.Write("Introduza o ID do Sensor: ");
                    string id = Console.ReadLine(); // Guarda o ID escrito
                    // Executa a Query usando o ID fornecido (WHERE SensorId = 'id')
                    ConsultarBaseDados($"SELECT * FROM Medicoes WHERE SensorId = '{id}' ORDER BY DataHora DESC LIMIT 20");
                }
                // Se escolheu 3, pergunta o Tipo (ex: TEMP) e pesquisa apenas por esse tipo de medição
                else if (opcao == "3")
                {
                    Console.Write("Introduza o Tipo de Dado: ");
                    string tipo = Console.ReadLine(); // Guarda o Tipo escrito
                    // Executa a Query usando o Tipo fornecido (WHERE Tipo = 'tipo')
                    ConsultarBaseDados($"SELECT * FROM Medicoes WHERE Tipo = '{tipo}' ORDER BY DataHora DESC LIMIT 20");
                }
                // Se escolheu 4, permite enviar dados falsos manuais para simular uma chamada RPC ao Python
                else if (opcao == "4")
                {
                    Console.Write("Introduza o Tipo de Dado a analisar (ex: TEMP): ");
                    string tipo = Console.ReadLine(); // Lê o tipo manual
                    Console.Write("Introduza o Valor (ex: 45): ");
                    string valor = Console.ReadLine(); // Lê o valor manual

                    Console.WriteLine("\n[A contactar o motor de IA Python via RPC...]");
                    // Faz a chamada remota (RPC) ao serviço Python enviando o tipo e o valor
                    string analise = ChamarAnaliseRPC(tipo, valor);
                    // Mostra o que o Python respondeu
                    Console.WriteLine($"[RESULTADO DA PREVISÃO]: {analise}");

                    // Pergunta se o utilizador quer gravar esta simulação na base de dados
                    Console.Write("Deseja guardar este cenário na Base de Dados? (s/n): ");
                    if (Console.ReadLine().ToLower() == "s") // Converte a resposta para minúscula e verifica se é 's'
                    {
                        // Guarda o dado na BD identificando o sensor como "MANUAL"
                        GuardarMedicaoBD("MANUAL", tipo, valor, analise);
                    }
                }
                // Se escolheu 0, encerra o programa de forma limpa
                else if (opcao == "0")
                {
                    Console.WriteLine("A encerrar o Servidor Central...");
                    Environment.Exit(0); // Mata o processo atual
                }
                // Se escreveu qualquer outra coisa, avisa que é inválido
                else
                {
                    Console.WriteLine("Opção inválida.");
                }
            }
        }

        // Função que é executada cada vez que a Gateway envia um dado novo (corre na sua própria Thread)
        static void ProcessarCliente(TcpClient client)
        {
            // Abre o "tubo" de comunicação (Stream) com a Gateway para ler os dados
            using (NetworkStream stream = client.GetStream())
            {
                // Cria um espaço temporário (buffer) de 1024 bytes para guardar os dados que vêm da rede
                byte[] buffer = new byte[1024];
                // Lê os dados da stream e guarda-os no buffer, registando quantos bytes foram lidos
                int bytesRead = stream.Read(buffer, 0, buffer.Length);

                // Se recebemos mais do que 0 bytes (ou seja, se a mensagem não está vazia)
                if (bytesRead > 0)
                {
                    // Converte os bytes puros recebidos numa string legível, utilizando codificação UTF-8
                    string msgRecebida = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    // Imprime a mensagem bruta recebida da Gateway na consola
                    Console.WriteLine($"\n[Recebido do Gateway]: {msgRecebida}");

                    // Divide a mensagem em pedaços sempre que encontrar o símbolo "|"
                    string[] parts = msgRecebida.Split('|');

                    // Verifica se a mensagem tem pelo menos 4 partes e começa pela palavra "DATA"
                    if (parts.Length >= 4 && parts[0] == "DATA")
                    {
                        // Extrai a segunda parte da mensagem (ID do Sensor)
                        string sensorId = parts[1];
                        // Extrai a terceira parte da mensagem (Tipo, ex: TEMP)
                        string tipo = parts[2];
                        // Extrai a quarta parte da mensagem (Valor numérico ou texto da câmara)
                        string valor = parts[3];

                        // Envia o Tipo e o Valor para o Python avaliar o perigo (RPC) e guarda a resposta
                        string analise = ChamarAnaliseRPC(tipo, valor);

                        // Guarda todos os dados (ID, Tipo, Valor e o Diagnóstico do Python) no SQLite local
                        GuardarMedicaoBD(sensorId, tipo, valor, analise);

                        // Imprime o resultado do risco médico para o utilizador ver
                        Console.WriteLine($"[ANÁLISE EXTERNA] Risco para a Saúde: {analise}");
                        // Repete a linha do menu de opções para a interface não ficar confusa
                        Console.Write("\nPressione ENTER para voltar ao menu ou introduza uma opção: ");
                    }
                }
            }
            // Fecha a ligação com a Gateway, pois esta mensagem já foi tratada
            client.Close();
        }

        // Função que cria a estrutura da Base de Dados caso seja a primeira vez a correr o programa
        static void InicializarBaseDados()
        {
            try
            {
                // Estabelece a ligação com o ficheiro SQLite
                using (var conn = new SQLiteConnection(connectionString))
                {
                    // Abre a ligação à BD
                    conn.Open();
                    // Código SQL para criar a Tabela 'Medicoes'. O comando IF NOT EXISTS garante que não apaga os dados se a tabela já existir
                    string query = @"CREATE TABLE IF NOT EXISTS Medicoes (
                                        Id INTEGER PRIMARY KEY AUTOINCREMENT, /* ID único que cresce sozinho */
                                        SensorId TEXT, /* A identificação do Sensor (Ex: S101) */
                                        Tipo TEXT, /* O tipo de leitura (Ex: TEMP, CAMERA) */
                                        Valor TEXT, /* O valor numérico ou o alerta */
                                        Analise TEXT, /* O resultado do RPC devolvido pelo Python */
                                        DataHora DATETIME DEFAULT CURRENT_TIMESTAMP /* A hora exata e automática em que o dado entrou */
                                    )";
                    // Prepara o comando SQL para ser executado nesta ligação
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        // Executa a query sem esperar dados de retorno (apenas cria a estrutura)
                        cmd.ExecuteNonQuery();
                    }
                }
                // Imprime a confirmação e o local físico onde o ficheiro foi criado
                Console.WriteLine($"[BD] Pronta a usar em: {caminhoBaseDados}");
            }
            catch (Exception ex) // Se a BD der erro (falta de permissões, etc), apanha o erro aqui
            {
                // Imprime qual foi o erro que aconteceu em vez de rebentar o servidor
                Console.WriteLine($"[Erro BD]: {ex.Message}");
            }
        }

        // Função que insere uma nova linha com os dados da medição na Base de Dados
        static void GuardarMedicaoBD(string sensorId, string tipo, string valor, string analise)
        {
            // BLOQUEIO MUTEX: Pede autorização para entrar. Se outra mensagem já estiver a escrever, esta fica à espera na fila.
            dbMutex.WaitOne();
            try
            {
                // Estabelece e abre a ligação ao ficheiro SQLite
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();
                    // Código SQL para Inserir (INSERT) valores nas colunas. Usamos @parametros para proteger contra Injeções de SQL.
                    string query = "INSERT INTO Medicoes (SensorId, Tipo, Valor, Analise) VALUES (@sid, @tipo, @val, @analise)";

                    // Prepara o comando para execução
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        // Substitui os @parametros da query pelos valores reais que vieram do argumento da função
                        cmd.Parameters.AddWithValue("@sid", sensorId);
                        cmd.Parameters.AddWithValue("@tipo", tipo);
                        cmd.Parameters.AddWithValue("@val", valor);
                        cmd.Parameters.AddWithValue("@analise", analise);
                        // Executa a inserção na base de dados
                        cmd.ExecuteNonQuery();
                    }
                }
                // Confirma visualmente que gravou com sucesso
                Console.WriteLine($"[BD] Medição de {sensorId} guardada com sucesso.");
            }
            catch (Exception ex) // Apanha problemas na escrita
            {
                Console.WriteLine($"[Erro ao Guardar]: {ex.Message}");
            }
            finally
            {
                // SUPER IMPORTANTE: Liberta o semáforo/cadeado. Quer grave com sucesso ou dê erro, deixa a próxima mensagem aceder à BD.
                dbMutex.ReleaseMutex();
            }
        }

        // Função para ler dados (SELECT) da base de dados e desenhá-los no ecrã como uma tabela
        static void ConsultarBaseDados(string query)
        {
            // BLOQUEIO MUTEX: Protege a BD para não estarmos a tentar ler enquanto alguém está a meio de uma escrita
            dbMutex.WaitOne();
            try
            {
                // Estabelece ligação e abre a Base de Dados
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();
                    // Prepara a Query fornecida pelo Menu (ex: SELECT * FROM Medicoes...)
                    using (var cmd = new SQLiteCommand(query, conn))
                    // Executa a Query em modo "Leitor" (Reader), que percorre os resultados linha a linha
                    using (var reader = cmd.ExecuteReader())
                    {
                        // Desenha o cabeçalho estético da tabela
                        Console.WriteLine("\n------------------------------------------------------------------------------------------");
                        // Alinha as colunas no ecrã usando formatação (ex: -5 significa alinhado à esquerda ocupando 5 espaços)
                        Console.WriteLine($"{"ID",-5} | {"DATA/HORA",-19} | {"SENSOR",-6} | {"TIPO",-5} | {"VALOR",-6} | {"ANÁLISE"}");
                        Console.WriteLine("------------------------------------------------------------------------------------------");

                        // Variável para saber se o sistema encontrou algo ou se a tabela está vazia
                        bool encontrouDados = false;

                        // Enquanto houver linhas no resultado da pesquisa, lê a próxima linha...
                        while (reader.Read())
                        {
                            encontrouDados = true; // Avisa que pelo menos um dado foi encontrado
                            // Extrai as colunas da linha atual do SQLite e imprime-as de forma alinhada no ecrã
                            Console.WriteLine($"{reader["Id"],-5} | {reader["DataHora"],-19} | {reader["SensorId"],-6} | {reader["Tipo"],-5} | {reader["Valor"],-6} | {reader["Analise"]}");
                        }

                        // Se leu tudo e a variável continuou falsa, escreve que não há dados disponíveis
                        if (!encontrouDados)
                        {
                            Console.WriteLine("Nenhum registo encontrado na base de dados com estes critérios.");
                        }
                        // Desenha a linha de fecho da tabela
                        Console.WriteLine("------------------------------------------------------------------------------------------\n");
                    }
                }
            }
            catch (Exception ex) // Trata erros que possam acontecer durante a consulta
            {
                Console.WriteLine($"[Erro na Consulta]: {ex.Message}");
            }
            finally
            {
                // Liberta o Mutex para o sistema poder voltar a guardar dados livremente
                dbMutex.ReleaseMutex();
            }
        }

        // Função que atua como Cliente RPC (Remote Procedure Call) para invocar a Análise no serviço Python
        static string ChamarAnaliseRPC(string tipo, string valor)
        {
            try
            {
                // O C# age como um cliente e inicia uma ligação TCP direta ao serviço Python no IP Local e Porta 8000
                using (TcpClient rpcClient = new TcpClient("127.0.0.1", 8000))
                // Abre o túnel (Stream) para falar com o Python
                using (NetworkStream stream = rpcClient.GetStream())
                {
                    // Constrói a mensagem padrão que o Python espera receber (Ex: PREVER|TEMP|39)
                    string msgRPC = $"PREVER|{tipo}|{valor}";
                    // Converte a string de texto para uma sequência de Bytes UTF-8 para circular na rede
                    byte[] dataOut = Encoding.UTF8.GetBytes(msgRPC);
                    // Envia os bytes para a porta 8000 (disparando a ação no Python)
                    stream.Write(dataOut, 0, dataOut.Length);

                    // Prepara um espaço na memória (buffer) para ouvir a resposta médica
                    byte[] dataIn = new byte[1024];
                    // O C# fica bloqueado aqui à espera que o Python processe o algoritmo e devolva os bytes do resultado
                    int bytesLidos = stream.Read(dataIn, 0, dataIn.Length);

                    // Converte os bytes recebidos de volta para texto legível (ex: "ALERTA: Insolação") e devolve à função principal
                    return Encoding.UTF8.GetString(dataIn, 0, bytesLidos);
                }
            }
            catch // Se o Python estiver desligado, ou a porta bloqueada, cai aqui
            {
                // Devolve uma mensagem de erro controlada para não rebentar o servidor C#
                return "Erro de ligação ao módulo de análise (Python).";
            }
        }
    }
}