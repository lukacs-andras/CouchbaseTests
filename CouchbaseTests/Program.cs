using System;
using System.Threading;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Core.Exceptions;
using Couchbase.KeyValue;
using Couchbase.Management.Views;
using Newtonsoft.Json.Linq;

namespace CouchbaseTests
{
    class Program
    {
        private const string CouchbaseConnectionString = "couchbase://localhost";
        private const string CouchbaseUsername = "admin";
        private const string CouchbasePassword = "TestTransactions";

        /// <summary>
        /// Egy user könyveinek címe ilyen property path-on érhető el a Person osztályon belül.
        /// Azért kis kezdőbetűs, mert a CB kis k ezdőbetűvel tárolja a prop-okat.
        /// </summary>
        private const string BookByTitlePropertyPath = "booksByTitle";

        static async Task Main(string[] args)
        {
            // TODO translate comments to English because this is a public repo

            /* A futtatáshoz szükséges egy Couchbase node, amire a CouchbaseConnectionString-el kell hivatkozni. */
            await TestCouchBase(false);
        }

        private static async Task TestCouchBase(bool runTestsThatThrow)
        {
            const string TestBucketName = "People";

            using (var cluster = await Cluster.ConnectAsync(CouchbaseConnectionString, CouchbaseUsername, CouchbasePassword))
            {
                if (runTestsThatThrow)
                {
                    try
                    {
                        var nonExistentBucket = await cluster.BucketAsync("nonExistentBucketName");
                    }
                    catch (Couchbase.Management.Buckets.BucketNotFoundException)
                    {
                        // van másik BucketNotFoundException is CB-ben!

                        // a CB kliens hülyén működik, mert ezután a kivétel után használhatatlan az ICluster példány:
                        // CreateBucket()-ben CreateBucketAsync() elhányja magát ezzel: Value cannot be null. (Parameter 'uri')
                    }
                }

                // az összes bucket lekérése és bucket törlése
                var buckets = await cluster.Buckets.GetAllBucketsAsync();
                if (buckets.ContainsKey(TestBucketName))
                {
                    await cluster.Buckets.DropBucketAsync(TestBucketName);
                }

                // bucket létrehozása
                await CreateBucket(cluster, TestBucketName);

                // dokumentum bucket-be insert-álása
                // ha rögtön a bucket létrehozása után írunk bele, a CB időnként elszáll hibával, mert nem tudja olyan gyorsan létrehozni a bucket-et. Várni kell pár másodpercet írás előtt.
                await Task.Delay(TimeSpan.FromSeconds(2));
                var bucket = await cluster.BucketAsync(TestBucketName);
                // a CB 3.0-ában lettek bevezetve a bucket-en belül a kollekciók
                var collection = bucket.DefaultCollection();
                string documentId = await InsertDocument(collection);

                // teljes dokumentum update-elése
                await UpdateDocument(collection, documentId);

                // teljes dokumentum lekérése
                Person person = await GetDocument(collection, documentId);

                // dokumentum részének lekérése
                string name = await GetPersonName(collection, documentId);
                string[] bookTitles = await GetPersonsBookTitles(collection, documentId);
                bool isHairColorAvailable = await PersonPropertyExists(collection, documentId, "hairColor");

                // dokumentum részének módosítása
                await UpdatePersonName(collection, documentId, person);
                string newBookTitle = "anglicko-slovenský praktický slovník";
                await AddBookToPerson(collection, documentId, newBookTitle);
                // nem adja hozzá
                await AddBookToPersonIfNotExists(collection, documentId, newBookTitle);
                // hozzáadja
                await AddBookToPersonIfNotExists(collection, documentId, "Dependency Injection in .NET");

                // konkurrencia CAS-al (compare and swap ID); pesszimista lock is létezik CB-ben
                await UpdateSamePersonUsingOptimisticLockingWithCasValue(collection, documentId);

                // dokumentum törlése
                await DeleteDocument(collection, documentId);

                /* Batch-elés: https://docs.couchbase.com/go-sdk/1.6/batching-operations.html
                 * A CB-nek nincs szerver oldali tranzakció kezelése. Ha batch-elve küldünk módosításokat a szerverre, előfordulhat,
                 * hogy egy része sikeres, más része sikertelen lesz.
                 * Az ajánlott batch méret 1 mb, amiben benne van a küldött doksik és az ID-ik (kulcsaik) mérete is, de lehet h gigabites hálózaton nagyobb méret is hatékony.
                 * 
                 * A .NET SDK doksi szerint úgy lehet batch-elni, hogy egymás után sok async CB műveletet adok ki. De ez nem adat batch-elés!
                 */

                // TODO a binary típusú doksik műveleteit is tesztelni
            }
        }

        private static async Task UpdateSamePersonUsingOptimisticLockingWithCasValue(ICouchbaseCollection collection, string documentId)
        {
            var getResult = await collection.GetAsync(documentId);
            var person = getResult.ContentAs<Person>();


            var newPerson1 = person.WithSomeNumber(1);
            var replaceOptions = new ReplaceOptions().Cas(getResult.Cas);
            // csak akkor replace-el, ha a doksi CAS-ja egyezik a ReplaceOptions-ben megadottal
            // az UpsertAsync()-nek beadható UpsertOption()-nek nincs Cas() művelete, ezért használom Replace()-t
            var mutationResult1 = await collection.ReplaceAsync(documentId, newPerson1, replaceOptions);

            try
            {
                var newPerson2 = person.WithSomeNumber(2);
                // a doksi CAS-ja nem egyezik a korábban lekérttel, ezért dob
                var mutationResult2 = await collection.ReplaceAsync(documentId, newPerson2, replaceOptions);
            }
            catch (Couchbase.Core.Exceptions.KeyValue.DocumentExistsException ex)
            {
                // a hiba részeleteit tartalmazza; típusa: Couchbase.Core.Exceptions.KeyValue.KeyValueErrorContext: több minden van benne, mint IErrorContext-en
                var replaceContext = ex.Context;
            }
        }

        private static async Task AddBookToPersonIfNotExists(ICouchbaseCollection collection, string documentId, string bookTitle)
        {
            // csak akkor adja hozzá, ha nincs a könyv címek között
            var mutateResult = await collection.MutateInAsync(documentId, builder => builder.ArrayAddUnique(BookByTitlePropertyPath, bookTitle));
        }

        private static async Task AddBookToPerson(ICouchbaseCollection collection, string documentId, string bookTitle)
        {
            // csupa kis betűvel (booksbytitle) nem csinál semmit, csak ha pontosan úgy adom meg, ahogy a property neve CB-ben szerepel
            // létezik ArrayPrepend() és ArrayInsert() is
            var mutateResult = await collection.MutateInAsync(documentId, builder => builder.ArrayAppend(BookByTitlePropertyPath, new[] { bookTitle }));
        }

        private static async Task UpdatePersonName(ICouchbaseCollection collection, string documentId, Person person)
        {
            var mutationResult = await collection.MutateInAsync(documentId, builder => builder.Upsert("name", "Mr. " + person.Name));
        }

        private static async Task<bool> PersonPropertyExists(ICouchbaseCollection collection, string documentId, string propertyName)
        {
            var lookupResult = await collection.LookupInAsync(documentId, builder => builder.Exists(propertyName));
            return lookupResult.ContentAs<bool>(0);
        }

        private static async Task<string[]> GetPersonsBookTitles(ICouchbaseCollection collection, string documentId)
        {
            var lookupResult = await collection.LookupInAsync(documentId, builder => builder.Get(BookByTitlePropertyPath));
            return lookupResult.ContentAs<string[]>(0);
        }

        private static async Task<string> GetPersonName(ICouchbaseCollection collection, string documentId)
        {
            // A) kis-nagybetű érzékeny: pontosan úgy kell írni a property nevét, ahogy a CB-ben van, különben null-t ad
            // B) ha property property-je kell, akkor lehet így: name.surname
            var getResult = await collection.LookupInAsync(documentId, specificationBuilder => specificationBuilder.Get("name"));
            return getResult.ContentAs<string>(0);
        }

        private static async Task DeleteDocument(ICouchbaseCollection collection, string documentId)
        {
            await collection.RemoveAsync(documentId);
        }

        private sealed class Person
        {
            public string Name { get; set; }
            public string Sex { get; set; }
            public string Status { get; set; }
            public string[] BooksByTitle { get; set; }
            public int SomeNumber { get; set; }

            /// <summary>
            /// Shallow copy-t csinál a példányról. Az új példányon SomeNumber-t a beadott értékre állítja.
            /// </summary>
            /// <param name="someNumber"></param>
            /// <returns></returns>
            public Person WithSomeNumber(int someNumber)
            {
                var newPerson = (Person)MemberwiseClone();
                newPerson.SomeNumber = someNumber;
                return newPerson;
            }
        }

        private static async Task<Person> GetDocument(ICouchbaseCollection collection, string documentId)
        {
            using (var result = await collection.GetAsync(documentId))
            {
                // a korábban INSERT-ált doksinak olyan property-jei vannak, mint a Person osztálynak, ezért működik
                var person = result.ContentAs<Person>();
                // azért működik, mert a CB kliens Newtonsoft JSON sorosítót használ, és JObject a lekért doksi eredeti nyers formája
                var jObject = result.ContentAs<JObject>();
                try
                {
                    // Az online tutorial-ban (https://docs.couchbase.com/dotnet-sdk/3.0/howtos/kv-operations.html) a result.ContentAs<string>() hívást mutatják, pedig nem jó...
                    // Elszáll azzal a hibával, hogy nem tudja parse-olni azt, hogy "{.Path"; azért, mert a JObject példányt akarja parse-olni (aminek van Path prop-ja),
                    // és megdöglik. A CB kliens itt is hülyén működik, mert a használt visszasorosítót hozzádrótozták az interface-hez. Legalább tettek volna
                    // IGetResult-ra egy GetAsRawString() metódust, és akkor nem kéne nekünk foglalkozni azzal, hogy milyen library-t használnak belsőleg, ha
                    // csak a doksi nyers string formátuma érdekel. Ehelyett a lenti módszerrel kell vacakolni: lekérni egy JObject-et, aztán azon hívni ToString()-et.
                    string rawDocumentString = result.ContentAs<string>();
                }
                catch (Exception ex)
                {
                }
                string documentRawString = jObject.ToString();
                return person;
            }
        }

        private static async Task UpdateDocument(ICouchbaseCollection collection, string documentId)
        {
            // hiába kezdődnek nagy betűvel a property nevek, a CB-be beírt JSON-ban kis betűvel kezdődnek; minden CB művelet kis-nagybetű érzékeny!
            var person = new
            {
                Name = "András 2",
                Sex = "male",
                Status = "offline",
                BooksByTitle = new[]
                {
                    "Tom Jones",
                    "Treasure Island",
                    "RoboCop 2"
                }
            };
            var mutationResult = await collection.UpsertAsync(documentId, person);
        }

        /// <summary>
        /// Inserts a document into a collection of a bucket, and returns the ID of the new document.
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        private static async Task<string> InsertDocument(ICouchbaseCollection collection)
        {
            // hiába kezdődnek nagy betűvel a property nevek, a CB-be beírt JSON-ban kis betűvel kezdődnek; minden CB művelet kis-nagybetű érzékeny!
            var person = new
            {
                Name = "András",
                Sex = "male",
                BirthDate = "1981-01-13"
            };
            string personId = $"person_{DateTime.UtcNow:yyyyMMddHHmmssfff}";
            // a válaszban van egy Cas property: ez a NoSQL compare and swap ID-je, ami az SQL optimistic concurrency ID-nek felel meg, vagyis csak akkor hajtja végre
            // a szerver az írást, ha a dokumentumhoz tartozó CAS érték nem változott a CAS kiolvasása (a doksi lekérése) óta
            var mutationResult = await collection.InsertAsync(
                personId,
                person,
                new InsertOptions().Timeout(TimeSpan.FromSeconds(3)));
            return personId;
        }

        private static async Task CreateBucket(ICluster cluster, string bucketName)
        {
            var bucketSettings = new Couchbase.Management.Buckets.BucketSettings
            {
                // mindenképp kitöltendő tulajdonságok
                Name = bucketName,
                BucketType = Couchbase.Management.Buckets.BucketType.Couchbase,
                // ennél nem lehet kevesebb
                RamQuotaMB = 100,
                FlushEnabled = false,

                // ha nem töltjük ki, akkor a CB a default beállítást használja
                CompressionMode = Couchbase.Management.Buckets.CompressionMode.Active,
                EjectionMethod = Couchbase.Management.Buckets.EvictionPolicyType.ValueOnly,
                ConflictResolutionType = Couchbase.Management.Buckets.ConflictResolutionType.SequenceNumber
            };
            // a CB kliens hibakezelése hulladék, mert ha pl. RamQuotaMB hibás értékű, ahelyett, hogy kihalászná a kérelem-specifikus hibát a HTTP válaszból,
            // egyszerűen annyival elhányja magát, hogy bocs, 400-as státusz kódú a HTTP válasz. A dobott kivételből semmi nem derül ki.
            // De ha pl. az a hiba, hogy létezik a létrehozandó nevű bucket, akkor normálisan beteszi a kivételbe a hibaüzenetet (mert az nem HTTP 400-at küld).
            await cluster.Buckets.CreateBucketAsync(bucketSettings);
        }
    }
}
