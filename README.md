<div dir="rtl">

# flutter_sync

محرّك مزامنة **Offline-First** لتطبيقات Flutter، يعمل مع قاعدة بياناتك وخادمك الحاليين.

التطبيق **يقرأ ويكتب دائمًا في قاعدة البيانات المحلية**. تسجّل `flutter_sync` كل تغيير في صندوق صادر دائم (outbox)،
وترسله إلى الخادم عند الإمكان، بالترتيب الصحيح ومرة واحدة فقط. كما تجلب تغييرات الخادم دون الكتابة فوق تعديلات لم تُرفع بعد.

- **رفع يراعي الاعتماديات:** لا تصل الفاتورة إلى الخادم قبل عميلها، ولا العميل قبل حسابه. وتُستبدل المعرّفات المحلية في المراجع بمعرّفات الخادم تلقائيًا، في مزامنة واحدة.
- **لا تخمين:** المعرّف المحلي الذي لم يحصل على معرّف خادم بعد لا يُرسل أبدًا على أنه معرّف خادم. السجل إمّا ينتظر السجل الأب، أو يفشل برسالة واضحة.
- **لا فقدان للبيانات:** العمليات الفاشلة تُحفظ ولا تُحذف، حتى تعيد المحاولة أو تصحح البيانات أو تتجاهلها. فترات انقطاع الإنترنت لا تستهلك محاولات إعادة الإرسال.
- **تسليم مرة واحدة فقط** عبر مفتاح idempotency يُرسل مع كل عملية، مع معالجة حالة «السجل موجود مسبقًا ← اعتمده».
- **الدمج (Coalescing):** إنشاء ثم تعديل يُرسلان كعملية إنشاء واحدة، وإنشاء ثم حذف لا يُرسل شيئًا.
- **سحب قابل للاستئناف:** صفحةً بصفحة مع مؤشر (cursor) محفوظ. تُسحب الجداول الأب أولًا، وتُحوّل معرّفات الخادم إلى معرّفات محلية، وتمرّ التعارضات على محلّل قابل للتخصيص.
- **مزامنة تلقائية** بعد الكتابة (مع تأخير بسيط لتجميع التغييرات)، وعند عودة الاتصال، وعند عودة التطبيق إلى الواجهة، ودوريًا، وعند الطلب. تعمل عملية مزامنة واحدة في كل مرة.
- **حالة للواجهة:** `ValueListenable<SyncStatus>`، وحالة لكل سجل، وتدفق أحداث.
- **بلا اعتماديات إلزامية:** استخدم drift أو sqflite أو أي SQLite للتخزين، و http أو dio أو غيرهما للشبكة.

## كيف تعمل

</div>

```
 UI ──reads/writes──▶ your local DB ──(same transaction)──▶ sync outbox
                            ▲                                   │
                            │ applyRemote            push, in order,
                            │ (pulled records)       ids rewritten
                            │                                   ▼
                        SyncEngine ◀────────── pull pages ──── server
```

<div dir="rtl">

لكل كيان (جدول) تقدّم:

| الجزء | وظيفته |
|---|---|
| `RemoteAdapter` | يرفع تغييرًا واحدًا ويسحب صفحة واحدة. يغطي `RestRemoteAdapter` معظم واجهات REST. |
| `LocalAdapter` | يكتب السجل المسحوب في جدولك ويستقبل معرّفات الخادم. |
| `SyncReference` | يحدد أي الحقول تشير إلى أي كيان (`customerId → customers`، `items[].itemId → items`). |

## البدء السريع

</div>

```dart
final sync = SyncEngine(
  store: SqlSyncStore(DriftSqlExecutor(db)), // أو InMemorySyncStore()
  connectivity: StreamConnectivity(
    initial: await InternetConnection().hasInternetAccess,
    changes: InternetConnection().onStatusChange
        .map((s) => s == InternetStatus.connected),
  ),
  config: SyncConfig(canSync: () => session.isValid),
  entities: [
    SyncEntityConfig(
      name: 'accounts',
      remote: RestRemoteAdapter(transport: transport, resource: 'accounts'),
      local: AccountsLocal(db),
      pullMode: PullMode.fullRefresh,
    ),
    SyncEntityConfig(
      name: 'customers',
      remote: RestRemoteAdapter(transport: transport, resource: 'customers'),
      local: CustomersLocal(db),
      references: [SyncReference('accountId', 'accounts')],
    ),
    SyncEntityConfig(
      name: 'bills',
      remote: RestRemoteAdapter(transport: transport, resource: 'bills'),
      local: BillsLocal(db),
      references: [
        SyncReference('customerId', 'customers'),
        SyncReference('items[].itemId', 'items'),
      ],
    ),
    // ...
  ],
);
await sync.start();
```

<div dir="rtl">

سجّل كل عملية كتابة محلية، داخل نفس المعاملة (transaction) التي تكتب فيها السجل:

</div>

```dart
await db.transaction(() async {
  final id = await db.into(db.customers).insert(row);
  await sync.recordCreate('customers', id, customer.toJson());
});

await sync.recordUpdate('customers', id, {'phone': '0100...'}); // يكفي إرسال الحقول المعدّلة
await sync.recordDelete('customers', id);
```

<div dir="rtl">

هذا كل شيء. يرفع المحرّك التغييرات بعد لحظات، أو عند عودة الاتصال.

### حقول المراجع تحمل المعرّفات المحلية

داخل قاعدة بياناتك وفي البيانات التي تسجّلها، يحمل حقل المرجع دائمًا **المعرّف المحلي** للسجل المُشار إليه.
يحوّله المحرّك إلى معرّف الخادم عند الرفع، ويحوّل معرّفات الخادم إلى محلية عند السحب.
لا تخزّن أبدًا معرّف خادم في عمود مرجع: هذا الخلط هو بالضبط ما يجعل السجلات ترتبط بالأب الخطأ.

أكثر المعرّفات المحلية أمانًا هي UUID عبر `SyncEngine.newLocalId()`، لكن المعرّفات الرقمية ذات الزيادة التلقائية تعمل أيضًا.

## عرض الحالة

</div>

```dart
SyncStatusBuilder(
  engine: sync,
  builder: (context, s) => switch (s.phase) {
    SyncPhase.offline => const Text('غير متصل – يتم حفظ التغييرات محليًا'),
    SyncPhase.authRequired => const Text('انتهت الجلسة، سجّل الدخول مجددًا'),
    _ when s.failedCount > 0 => Text('${s.failedCount} عملية تحتاج مراجعة'),
    _ when s.pendingCount > 0 => Text('${s.pendingCount} بانتظار الرفع'),
    _ => const Text('تمت المزامنة'),
  },
);

RecordSyncStateBuilder(
  engine: sync, entity: 'bills', localId: bill.id,
  builder: (context, state) => state == RecordSyncState.synced
      ? const SizedBox()
      : const Icon(Icons.cloud_off, size: 14),
);
```

<div dir="rtl">

تبقى العمليات الفاشلة في الصندوق الصادر مع سبب الفشل:

</div>

```dart
for (final op in await sync.failedOperations()) {
  print('${op.entity}/${op.localId}: ${op.lastError}');
}
await sync.retryFailed();          // الكل، أو retryFailed(op.id)
await sync.discard(op.id);         // التخلي عن تغيير واحد
```

<div dir="rtl">

تعديل سجل فشلت آخر عملية له يدمج التصحيح في تلك العملية ويعيدها إلى قائمة الانتظار.
هذه هي الحالة المعتادة لخطأ تحقق (validation) يصححه المستخدم.

## كتابة المحوّلات (Adapters)

### المحوّل المحلي

</div>

```dart
class CustomersLocal extends LocalAdapter {
  CustomersLocal(this.db);
  final AppDatabase db;

  @override
  Future<String> applyRemote(Map<String, dynamic> record,
      {required String? localId, required String serverId}) async {
    // حقول المراجع في `record` تحمل المعرّفات المحلية مسبقًا.
    final row = CustomersCompanion(
      id: localId == null ? const Value.absent() : Value(int.parse(localId)),
      name: Value(record['name']),
      accountId: Value(record['accountId']),
      serverId: Value(int.parse(serverId)),
    );
    final id = await db.into(db.customers).insertOnConflictUpdate(row);
    return '${localId ?? id}';
  }

  @override
  Future<void> applyRemoteDelete(String localId, {required String serverId}) =>
      (db.delete(db.customers)..where((c) => c.id.equals(int.parse(localId)))).go();

  // اختياري: تحديث عمود server_id الخاص بك.
  @override
  Future<void> onServerIdAssigned(String localId, String serverId,
          {Map<String, dynamic>? serverRecord}) =>
      db.customStatement('UPDATE customers SET server_id = ? WHERE id = ?',
          [int.parse(serverId), int.parse(localId)]);

  // اختياري: سجلات أُنشئت قبل تثبيت flutter_sync.
  @override
  Future<String?> findServerId(String localId) async => (await db
          .customSelect('SELECT server_id FROM customers WHERE id = ?',
              variables: [Variable(int.parse(localId))])
          .getSingleOrNull())
      ?.data['server_id']
      ?.toString();
}
```

<div dir="rtl">

عندما تُنشئ نقطة نهاية واحدة عدة سجلات دفعة واحدة (مثل حساب + ملف شخصي + عميل)،
سجّل كيان هذا الاستدعاء نفسه بـ `recordCreate`، وصرّح بالسجلات الأخرى عبر
`recordCreatedVia('customers', id, viaEntity: ..., viaLocalId: ...)`.
السجلات التي تشير إليها تنتظر حتى يكتمل هذا الرفع. بعد ذلك سجّل المعرّفات المُعادة عبر `registerMapping` داخل `onServerIdAssigned`.

استخدم `buildPushPayload` لإرسال أحدث البيانات (مثل فاتورة مع بنودها الحالية) بدلًا من البيانات المسجّلة مع العملية.

### المحوّل البعيد

يربط `RestRemoteAdapter` الإنشاء والتعديل والحذف بـ `POST` و `PUT` و `DELETE`، ويرسل ترويسة مفتاح idempotency،
ويفهم الردود من نوع `{"success": false, "message": ...}` التي تعود مع HTTP 200. طبقة النقل من اختيارك:

</div>

```dart
Future<RestResponse> transport(RestRequest r) async {
  final uri = Uri.parse('$baseUrl/${r.path}')
      .replace(queryParameters: r.query.isEmpty ? null : r.query);
  final request = http.Request(r.method, uri)
    ..headers.addAll({'Content-Type': 'application/json', ...auth(), ...r.headers});
  if (r.body != null) request.body = jsonEncode(r.body);
  final response = await http.Response.fromStream(await client.send(request));
  return RestResponse(
    statusCode: response.statusCode,
    body: response.body.isEmpty ? null : jsonDecode(utf8.decode(response.bodyBytes)),
    headers: response.headers,
  );
}
```

<div dir="rtl">

اترك استثناءات النقل (`SocketException` و `TimeoutException` و `ClientException`) تمرّ كما هي.
يعتبرها المحرّك «غير متصل» ولا يحسبها محاولة. **لا** تحوّلها إلى ردود HTTP وهمية.

لأي واجهة غير معتادة، نفّذ `RemoteAdapter` مباشرة وأعد `PushOutcome`:

| النتيجة | متى | الأثر |
|---|---|---|
| `success(serverId:)` | قُبل التغيير (أو موجود مسبقًا مع معرّفه) | تُحذف العملية ويُربط المعرّف |
| `networkError` | الخادم غير قابل للوصول | تتوقف المزامنة دون احتساب محاولة |
| `retry` | 5xx أو 429 أو انتهاء المهلة | تأخير متزايد ثم إعادة، وتُحتسب محاولة |
| `rejected` | خطأ تحقق | تُعلَّم العملية فاشلة، والسجلات التابعة تنتظر |
| `conflict` | لدى الخادم نسخة أحدث | يقرر محلّل التعارض |
| `unauthorized` | انتهت صلاحية الرمز | تتوقف المزامنة، `SyncPhase.authRequired` |

### مخزن SQL

ينشئ `SqlSyncStore` ثلاثة جداول (`sync_outbox` و `sync_id_map` و `sync_meta`) باستخدام `IF NOT EXISTS`،
فلا يحتاج مخططك إلى أي ترحيل (migration). ضعه في نفس قاعدة بيانات بياناتك ليُحفظ السجل وعمليته في الصندوق الصادر معًا.

- **drift:** راجع تعليق التوثيق في `SqlExecutor`، خمسة أسطر فقط.
- **sqflite:** وجّه الاستعلامات إلى المعاملة المفتوحة عبر Zone. راجع `SqfliteExecutor` في `test/sql_store_test.dart`.

## السحب

| `PullMode` | متى تستخدمه |
|---|---|
| `incremental` | عندما تدعم الواجهة الفلترة بوقت التعديل. استخدمه مع `UpdatedSincePagination`، ويبقى المؤشر محفوظًا بعد إعادة التشغيل. |
| `fullRefresh` | عندما لا تدعم الواجهة إلا التصفح الكامل. استخدمه مع `PagePagination`. يستقبل `LocalAdapter.onFullRefreshComplete` كل معرّفات الخادم التي ظهرت، لتحذف السجلات المحذوفة من الخادم. |
| `none` | كيانات للرفع فقط |

تُسحب الكيانات الأب أولًا، بترتيب مستنتج من `references` و `dependsOn`. ترتيب التسجيل لا يهم.

## التعارضات

التعارض هو سجل مسحوب ما زالت نسخته المحلية تحمل تغييرات لم تُرفع، أو عملية رفع ردّ عليها الخادم بـ `conflict`.

- `ConflictStrategy.keepLocal` (الافتراضي): يفوز تغيير الجهاز ويُرفع.
- `ConflictStrategy.serverWins`: تُعتمد نسخة الخادم ويُلغى التغيير المحلي.
- `ConflictStrategy.lastWriteWins`: يقارن `updatedAtOf(record)` بوقت التغيير المحلي.
- `conflictResolver: (c) => ConflictResolution.merge({...})` للدمج على مستوى الحقول.

## الإعدادات

</div>

```dart
SyncConfig(
  retryPolicy: RetryPolicy(maxAttempts: 8, baseDelay: Duration(seconds: 2)),
  debounce: Duration(milliseconds: 800),
  periodicInterval: Duration(minutes: 5),
  syncOnStart: true, syncOnResume: true, syncOnReconnect: true,
  autoPushAfterWrite: true,
  canSync: () => auth.isLoggedIn,
  onOperationFailed: (op, error) => showSnack('${op.entity}: $error'),
  logger: (level, message, [error, stack]) => debugPrint('[sync] $message'),
);
```

<div dir="rtl">

من إعدادات كل كيان في `SyncEntityConfig`: `pullPageSize` و `pushEnabled` و `coalesce` و `mergePayload`
و `serverIdOf` و `updatedAtOf` و `isDeletedOf` (للسجلات المحذوفة من الخادم) و `unknownReferencePolicy`.

## ترحيل تطبيق قائم

1. سجّل الصفوف الموجودة مرة واحدة:
   `await sync.registerMapping('customers', row.id, row.serverId)`،
   أو نفّذ `LocalAdapter.findServerId`/`findLocalId` لقراءة عمود `server_id` عند الحاجة.
2. أضف إلى قائمة الانتظار الصفوف التي لم تُرفع قط (`is_sync = 0`) عبر `recordCreate`/`recordUpdate`.
3. مرّر كل عمليات الإنشاء والتعديل والحذف في التطبيق عبر قاعدة البيانات المحلية مع `record*`،
   واحذف منطق «إن كان متصلًا استدعِ الـ API، وإلا احفظ محليًا».
4. استبدل شاشة «مزامنة كل شيء» اليدوية بـ `sync.syncNow()` مع `SyncStatusBuilder`.


</div>
