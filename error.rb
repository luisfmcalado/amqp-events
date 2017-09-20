module Error
  def self.exit_with_errors(errors)
    print "Errors found:\n"
    errors.each { |error| print "\t#{error}\n" }
    exit
  end
end

