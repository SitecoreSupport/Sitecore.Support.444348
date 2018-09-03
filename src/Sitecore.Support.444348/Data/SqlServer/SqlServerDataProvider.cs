using System;
using System.Collections.Generic;
using Sitecore.Data.SqlServer;
using System.Linq;
using System.Web;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Diagnostics;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Data.Templates;
using Sitecore.Data.DataProviders;
using Sitecore.Configuration;

namespace Sitecore.Support.Data.SqlServer
{
  public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    // Fields
    private readonly SqlDataApi api;

    // Methods
    public SqlServerDataProvider(string connectionString) : base(connectionString)
    {
      this.api = new SqlServerDataApi(connectionString);
    }

    protected bool CheckIfBlobShouldBeDeleted(FieldChange change)
    {
      Assert.ArgumentNotNull(change, "change");
      if ((change.IsBlob && (change.Value != change.OriginalValue)) && ID.IsID(change.OriginalValue))
      {
        object[] parameters = new object[] { "blobId", change.OriginalValue };
        return !base.Exists("\r\n          IF EXISTS (SELECT TOP 1 {0}Id{1} FROM {0}SharedFields{1} WHERE {0}Value{1} LIKE {2}blobId{3})\r\n            BEGIN\r\n              SELECT 1\r\n            END\r\n          ELSE\r\n            BEGIN\r\n  \r\n              IF EXISTS (SELECT TOP 1 {0}Id{1} FROM {0}VersionedFields{1} WHERE {0}Value{1} LIKE {2}blobId{3})\r\n                BEGIN\r\n                  SELECT 1\r\n                END\r\n              ELSE\r\n                BEGIN\r\n    \r\n                IF EXISTS (SELECT TOP 1 {0}FieldId{1} FROM {0}ArchivedFields{1} WHERE {0}Value{1} LIKE {2}blobId{3})\r\n                  BEGIN\r\n                    SELECT 1\r\n                  END\r\n  \r\n                END\r\n            END", parameters);
      }
      return false;
    }

    protected void ClearLanguageCache(ID templateId)
    {
      if (templateId == TemplateIDs.Language)
      {
        base.Languages = null;
      }
    }

    protected DefaultFieldSharing.SharingType GetSharingType(FieldChange change)
    {
      TemplateField definition = change.Definition;
      if (definition == null)
      {
        return DefaultFieldSharing.Sharing[change.FieldID];
      }
      return base.GetSharingType(definition);
    }

    protected void OnItemSaved(ID itemId, ID templateId)
    {
      base.RemovePrefetchDataFromCache(itemId);
      this.ClearLanguageCache(templateId);
    }

    protected void RemoveField(ID itemId, FieldChange change)
    {
      List<string> list = new List<string>();
      DefaultFieldSharing.SharingType sharingType = this.GetSharingType(change);
      switch (sharingType)
      {
        case DefaultFieldSharing.SharingType.Unknown:
        case DefaultFieldSharing.SharingType.Versioned:
          list.Add("DELETE FROM {0}VersionedFields{1}\r\n                WHERE {0}ItemId{1} = {2}itemId{3}\r\n                AND {0}Version{1} = {2}version{3}\r\n                AND {0}FieldId{1} = {2}fieldId{3}\r\n                AND {0}Language{1} = {2}language{3}");
          break;
      }
      if ((sharingType == DefaultFieldSharing.SharingType.Shared) || (sharingType == DefaultFieldSharing.SharingType.Unknown))
      {
        list.Add(" DELETE FROM {0}SharedFields{1}\r\n                 WHERE {0}ItemId{1} = {2}itemId{3}\r\n                 AND {0}FieldId{1} = {2}fieldId{3}");
      }
      if ((sharingType == DefaultFieldSharing.SharingType.Unversioned) || (sharingType == DefaultFieldSharing.SharingType.Unknown))
      {
        list.Add(" DELETE FROM {0}UnversionedFields{1}\r\n                 WHERE {0}ItemId{1} = {2}itemId{3}\r\n                 AND {0}FieldId{1} = {2}fieldId{3}\r\n                 AND {0}Language{1} = {2}language{3}");
      }
      foreach (string str in list)
      {
        this.Api.Execute(str, new object[] { "itemId", itemId, "fieldId", change.FieldID, "language", change.Language, "version", change.Version });
      }
    }

    private void RemoveOldBlobs(ItemChanges changes, CallContext context)
    {
      foreach (FieldChange change in changes.FieldChanges)
      {
        if (this.CheckIfBlobShouldBeDeleted(change))
        {
          this.RemoveBlobStream(new Guid(change.OriginalValue), context);
        }
      }
    }

    public override bool SaveItem(ItemDefinition itemDefinition, ItemChanges changes, CallContext context)
    {
      Action action2 = null;
      Action action = null;
      if (changes.HasPropertiesChanged || changes.HasFieldsChanged)
      {
        if (action == null)
        {
          if (action2 == null)
          {
            action2 = delegate {
              using (DataProviderTransaction transaction = this.Api.CreateTransaction())
              {
                if (changes.HasPropertiesChanged)
                {
                  this.UpdateItemDefinition(itemDefinition, changes);
                }
                if (changes.HasFieldsChanged)
                {
                  this.UpdateItemFields(itemDefinition.ID, changes);
                }
                transaction.Complete();
              }
            };
          }
          action = action2;
        }
        Factory.GetRetryer().ExecuteNoResult(action);
      }
      this.RemoveOldBlobs(changes, context);
      this.OnItemSaved(itemDefinition.ID, itemDefinition.TemplateID);
      return true;
    }

    protected void UpdateItemDefinition(ItemDefinition item, ItemChanges changes)
    {
      string str = StringUtil.GetString(changes.GetPropertyValue("name"), item.Name);
      ID id = MainUtil.GetObject(changes.GetPropertyValue("templateid"), item.TemplateID) as ID;
      ID id2 = MainUtil.GetObject(changes.GetPropertyValue("branchid"), item.BranchId) as ID;
      string sql = " UPDATE {0}Items{1} SET {0}Name{1} = {2}name{3}, {0}TemplateID{1} = {2}templateID{3}, {0}MasterID{1} = {2}branchId{3}, {0}Updated{1} = {2}now{3} WHERE {0}ID{1} = {2}itemID{3}";
      this.Api.Execute(sql, new object[] { "itemID", item.ID, "name", str, "templateID", id, "branchId", id2, "now", DateTime.UtcNow });
    }

    protected void UpdateItemFields(ID itemId, ItemChanges changes)
    {
      lock (base.GetLock(itemId))
      {
        DateTime now = DateTime.UtcNow;
        bool saveAll = changes.Item.RuntimeSettings.SaveAll;
        if (saveAll)
        {
          this.RemoveFields(itemId, changes.Item.Language, changes.Item.Version);
        }
        DefaultFieldSharing.SharingType[] typeArray = new DefaultFieldSharing.SharingType[] { DefaultFieldSharing.SharingType.Shared, DefaultFieldSharing.SharingType.Unversioned, DefaultFieldSharing.SharingType.Versioned };
        foreach (DefaultFieldSharing.SharingType type in typeArray)
        {
          foreach (FieldChange change in changes.FieldChanges)
          {
            if (this.GetSharingType(change) == type)
            {
              if (change.RemoveField)
              {
                if (!saveAll)
                {
                  this.RemoveField(itemId, change);
                }
              }
              else
              {
                switch (type)
                {
                  case DefaultFieldSharing.SharingType.Shared:
                    this.WriteSharedField(itemId, change, now, saveAll);
                    break;

                  case DefaultFieldSharing.SharingType.Unversioned:
                    this.WriteUnversionedField(itemId, change, now, saveAll);
                    break;

                  case DefaultFieldSharing.SharingType.Versioned:
                    this.WriteVersionedField(itemId, change, now, saveAll);
                    break;
                }
              }
            }
          }
        }
      }
    }

    // Properties
    public SqlDataApi Api =>
        this.api;
  }
}