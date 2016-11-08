using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Models;
using Newtonsoft.Json;

namespace Processa_Pedidos
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue cloudQueue;

        public WorkerRole()
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=trabcomputacaonuvem;AccountKey=BSG948CYnFDruuuFlUws6rNGI4eYubMShV7AszlYRC/JWQkSNoY1wGsWfqICqnlgv7KHj8W82VkdDi0H+d38+w==";

            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Trace.TraceInformation("Deu erro");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            cloudQueue = cloudQueueClient.GetQueueReference("demoqueue");

            // Note: Usually this statement can be executed once during application startup or maybe even never in the application.
            //       A queue in Azure Storage is often considered a persistent item which exists over a long time.
            //       Every time .CreateIfNotExists() is executed a storage transaction and a bit of latency for the call occurs.
            cloudQueue.CreateIfNotExists();
        }

        public override void Run()
        {
            Trace.TraceInformation("Processa Pedidos is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("Processa Pedidos has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Processa Pedidos is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Processa Pedidos has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");

                GetMessageFromQueue();

                await Task.Delay(1000);
            }
        }

        public void GetMessageFromQueue()
        {
            var cloudQueueMessage = cloudQueue.GetMessage();

            if (cloudQueueMessage == null)
            {
                return;
            }

            Pedido pedido;

            try
            {
                pedido = JsonConvert.DeserializeObject<Pedido>(cloudQueueMessage.AsString);
                Trace.TraceInformation(cloudQueueMessage.AsString);
            } catch (Exception e) {
                Trace.TraceInformation("It is not a Pedido object");
            }
            

            cloudQueue.DeleteMessage(cloudQueueMessage);
        }

      
    }
}
