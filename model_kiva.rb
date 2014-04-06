require 'sequel'

# DB = Sequel.connect(:adapter=>'mysql', :host=>'localhost', :database=>'kiva.org', :user=>'root')
# DB = Sequel.connect(:adapter=>'mysql', :host=>'localhost', :database=>'kiva.org', :user=>'root')
DB = Sequel.connect('jdbc:mysql://localhost/kiva?user=root')

DB.drop_table?(:lender)

DB.create_table?(:lender) do
  column :lender_id,          String, :primary_key => true, :index => true
  column :name,               String
  column :image_id,           Integer
  column :image_template_id,  Integer
  column :whereabouts,        String
  column :country_code,       String
  column :uid,                String
  column :member_since,       DateTime
  column :personal_url,       String
  column :occupation,         String
  column :loan_because,       String
  column :occupational_info,  String
  column :loan_count,         Integer
  column :invitee_count,      Integer
  column :inviter_id,         String
end

class Lender < Sequel::Model(:lender)
  unrestrict_primary_key
end

Lender.new(lender_id: "joe").save


