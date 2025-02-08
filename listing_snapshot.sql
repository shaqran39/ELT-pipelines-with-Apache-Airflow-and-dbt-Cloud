{% snapshot listing_snapshot %}

{{
        config(
          strategy= 'timestamp',
          unique_key='LISTING_ID',
          alias='listing_snapshot',
          updated_at='scraped_date'
        )
    }}

select * from {{ ref('s_listing') }}

{% endsnapshot %}