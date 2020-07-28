namespace Cosmos.Samples.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;


    class Program
    {
        //Read configuration
        private static readonly string databaseId = "samples";
        private static readonly string containerId = "container-samples";
        private static readonly JsonSerializer Serializer = new JsonSerializer();
        private static readonly List<string> partitionKeyPath = new List<string> { "/AccountNumber", "/id" };

        private static Database database = null;
        private static Container container = null;

        // Async main requires c# 7.1 which is set in the csproj with the LangVersion attribute
        // <Main>
        public static async Task Main(string[] args)
        {
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json")
                    .Build();

                string endpoint = configuration["EndPointUrl"];
                if (string.IsNullOrEmpty(endpoint))
                {
                    throw new ArgumentNullException("Please specify a valid endpoint in the appSettings.json");
                }

                string authKey = configuration["AuthorizationKey"];
                if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
                {
                    throw new ArgumentException("Please specify a valid AuthorizationKey in the appSettings.json");
                }

                //Read the Cosmos endpointUrl and authorisationKeys from configuration
                //These values are available from the Azure Management Portal on the Cosmos Account Blade under "Keys"
                //NB > Keep these values in a safe & secure location. Together they provide Administrative access to your Cosmos account
                using (CosmosClient client = new CosmosClient(endpoint, authKey))
                {
                    await Program.RunSubpartitioningDemo(client);
                }
            }
            catch (CosmosException cre)
            {
                Console.WriteLine(cre.ToString());
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // </Main>

        /// <summary>
        /// Run through basic container access methods as a console app demo.
        /// </summary>
        /// <returns></returns>
        // <RunSubpartitioningDemo>
        private static async Task RunSubpartitioningDemo(CosmosClient client)
        {
            // Create the database if necessary
            await Program.Setup(client);

            await Program.CreateContainer();

            IList<SalesOrder> saleOrders =  await Program.CreateItemsAsync();

            await Program.ReadItemAsync(saleOrders);

            await Program.QueryItems();

            Program.Cleanup();
        }
        // </RunSubpartitioningDemo>

        private static async Task Setup(CosmosClient client)
        {
            database = await client.CreateDatabaseIfNotExistsAsync(databaseId);
        }

        // <CreateContainer>
        private static async Task CreateContainer()
        {
            //NEW! ContainerProperties constructor now accepts a List of partition key paths.
            ContainerProperties containerProperties = new ContainerProperties(containerId, partitionKeyPath);
            container = await database.CreateContainerAsync(containerProperties);

            Console.WriteLine($"{Environment.NewLine}1.1. Created container :{container.Id} with {partitionKeyPath.Count} Partition Keys: {string.Join(",", partitionKeyPath.ToArray())}");
        }
        // </CreateContainer>

        // <CreateItemsAsync>
        private static async Task<IList<SalesOrder>> CreateItemsAsync()
        {
            Console.WriteLine("\n1.1 - Creating items");

            List<SalesOrder> saleOrders = new List<SalesOrder>();
            SalesOrder salesOrder = GetSalesOrderSample("SalesOrder1","Account1");
            // NEW! To create PartitionKey object with multiple PartitionKeyPaths, use 
            // PartitionKeyBuilder to add the partitionkey values and call Build() to get
            // a PartitionKey Object.
            PartitionKey partitionKey = new PartitionKeyBuilder()
                .Add(salesOrder.AccountNumber)
                .Add(salesOrder.Id)
                .Build();
            ItemResponse<SalesOrder> response = await container.CreateItemAsync(salesOrder, partitionKey);
            SalesOrder salesOrder1 = response;
            Console.WriteLine($"\n PartitionKey Created: {partitionKey.ToString()}");
            Console.WriteLine($"\n1.1.1 - Item created {salesOrder1.Id}");
            saleOrders.Add(salesOrder1);

            // Also you can also create the item using just the object, we will  
            // extract the right partitionkey from the item.
            SalesOrder salesOrder2 = GetSalesOrderSample("SalesOrder2","Account1");
            ItemResponse<SalesOrder> response2 = await container.CreateItemAsync(salesOrder2);

            Console.WriteLine($"\n1.1.2 - Item created {salesOrder2.Id}");
            saleOrders.Add(salesOrder2);

            SalesOrder salesOrder3 = GetSalesOrderSample("SalesOrder3","Account1");
            using (Stream stream = Program.ToStream<SalesOrder>(salesOrder3))
            {
                PartitionKey partitionKey3 = new PartitionKeyBuilder()
                      .Add(salesOrder3.AccountNumber)
                      .Add(salesOrder3.Id)
                      .Build();
                using (ResponseMessage responseMessage = await container.CreateItemStreamAsync(stream, partitionKey3))
                {
                    // Item stream operations do not throw exceptions for better performance
                    if (responseMessage.IsSuccessStatusCode)
                    {
                        SalesOrder streamResponse = FromStream<SalesOrder>(responseMessage.Content);
                        Console.WriteLine($"\n1.1.2 - Item created {streamResponse.Id}");
                    }
                    else
                    {
                        Console.WriteLine($"Create item from stream failed. Status code: {responseMessage.StatusCode} Message: {responseMessage.ErrorMessage}");
                    }
                }
            }

            saleOrders.Add(salesOrder3);
            return saleOrders;
        }
        // </CreateItemsAsync>

        // <ReadItemAsync>
        private static async Task ReadItemAsync(IList<SalesOrder> salesOrders)
        {
            Console.WriteLine("\n1.2 - Reading Item by Id");

            // Note that Reads require a partition key to be specified.
            PartitionKey partitionKey = new PartitionKeyBuilder()
                .Add(salesOrders[0].AccountNumber)
                .Add(salesOrders[0].Id)
                .Build();
            ItemResponse<SalesOrder> response = await container.ReadItemAsync<SalesOrder>(
                partitionKey: partitionKey,
                id: salesOrders[0].Id);

            Console.WriteLine("Item read by Id {0}", response.Resource);
            // Read the same item but as a stream.
            using (ResponseMessage responseMessage = await container.ReadItemStreamAsync(
                partitionKey: partitionKey,
                id: salesOrders[0].Id))
            {
                // Item stream operations do not throw exceptions for better performance
                if (responseMessage.IsSuccessStatusCode)
                {
                    SalesOrder streamResponse = FromStream<SalesOrder>(responseMessage.Content);
                    Console.WriteLine($"\n1.2.2 - Item Read {streamResponse.Id}");
                }
                else
                {
                    Console.WriteLine($"Read item from stream failed. Status code: {responseMessage.StatusCode} Message: {responseMessage.ErrorMessage}");
                }
            }
        }
        // </ReadItemAsync>

        // <QueryItems>
        private static async Task QueryItems()
        {
            Console.WriteLine("\n1.4 - Querying for a item using its AccountNumber property");

            QueryDefinition query = new QueryDefinition(
                "select * from sales s where s.AccountNumber = @AccountInput ")
                .WithParameter("@AccountInput", "Account1");

            List<SalesOrder> allSalesForAccount1 = new List<SalesOrder>();
            using (FeedIterator<SalesOrder> resultSet = container.GetItemQueryIterator<SalesOrder>(query))
            {
                while (resultSet.HasMoreResults)
                {
                    FeedResponse<SalesOrder> response = await resultSet.ReadNextAsync();
                    SalesOrder sale = response.First();
                    Console.WriteLine($"\n1.4.1 Account Number: {sale.AccountNumber}; Id: {sale.Id};");
                    allSalesForAccount1.AddRange(response);
                }
            }

            Console.WriteLine($"\n1.4.2 Query found {allSalesForAccount1.Count} items.");
            // Use the same query as before but get the cosmos response message to access the stream directly
            List<SalesOrder> allSalesForAccount1FromStream = new List<SalesOrder>();
            using (FeedIterator streamResultSet = container.GetItemQueryStreamIterator(query))
            {
                while (streamResultSet.HasMoreResults)
                {
                    using (ResponseMessage responseMessage = await streamResultSet.ReadNextAsync())
                    {
                        // Item stream operations do not throw exceptions for better performance
                        if (responseMessage.IsSuccessStatusCode)
                        {
                            dynamic streamResponse = FromStream<dynamic>(responseMessage.Content);
                            List<SalesOrder> salesOrders = streamResponse.Documents.ToObject<List<SalesOrder>>();
                            Console.WriteLine($"\n1.4.3 - Item Query via stream {salesOrders.Count}");
                            allSalesForAccount1FromStream.AddRange(salesOrders);
                        }
                        else
                        {
                            Console.WriteLine($"Query item from stream failed. Status code: {responseMessage.StatusCode} Message: {responseMessage.ErrorMessage}");
                        }
                    }
                }
            }

            Console.WriteLine($"\n1.4.4 Query found {allSalesForAccount1FromStream.Count} items.");

            if (allSalesForAccount1.Count != allSalesForAccount1FromStream.Count)
            {
                throw new InvalidDataException($"Both query operations should return the same list");
            }
        }
        // </QueryItems>

        private static void Cleanup()
        {
            if (database != null)
            {
                database.DeleteAsync().Wait();
            }
        }

        private static Stream ToStream<T>(T input)
        {
            MemoryStream streamPayload = new MemoryStream();
            using (StreamWriter streamWriter = new StreamWriter(streamPayload, encoding: Encoding.Default, bufferSize: 1024, leaveOpen: true))
            {
                using (JsonWriter writer = new JsonTextWriter(streamWriter))
                {
                    writer.Formatting = Newtonsoft.Json.Formatting.None;
                    Program.Serializer.Serialize(writer, input);
                    writer.Flush();
                    streamWriter.Flush();
                }
            }

            streamPayload.Position = 0;
            return streamPayload;
        }

        private static T FromStream<T>(Stream stream)
        {
            using (stream)
            {
                if (typeof(Stream).IsAssignableFrom(typeof(T)))
                {
                    return (T)(object)stream;
                }

                using (StreamReader sr = new StreamReader(stream))
                {
                    using (JsonTextReader jsonTextReader = new JsonTextReader(sr))
                    {
                        return Program.Serializer.Deserialize<T>(jsonTextReader);
                    }
                }
            }
        }

        private static decimal NextDecimal(Random rng)
        {
            return new decimal(rng.Next(),
                               rng.Next(),
                               rng.Next(0x204FCE5E),
                               false,
                               0);
        }

        private static DateTime RandomTime(Random rng)
        {
            DateTime start = new DateTime(2013, 1, 1, 1, 1, 1);

            int range = (DateTime.Today - start).Hours;
            return start.AddHours(rng.Next(range));
        }

        private static SalesOrder GetSalesOrderSample(string itemId,string accountNumber)
        {
            Random rand = new Random();
            SalesOrder salesOrder = new SalesOrder
            {
                Id = itemId,
                AccountNumber = accountNumber,
                PurchaseOrderNumber = accountNumber+itemId,
                OrderDate = RandomTime(rand),
                SubTotal = NextDecimal(rand),
                TaxAmount = NextDecimal(rand),
                Freight = NextDecimal(rand),
                Items = new SalesOrderDetail[]
                {
                    new SalesOrderDetail
                    {
                        OrderQty = 1,
                        ProductId = 760,
                        UnitPrice = 419.4589m,
                        LineTotal = 419.4589m
                    }
                },
            };

            // Set the "ttl" property to auto-expire sales orders in 30 days 
            salesOrder.TimeToLive = 60 * 60 * 24 * 30;

            return salesOrder;
        }
    }
}
